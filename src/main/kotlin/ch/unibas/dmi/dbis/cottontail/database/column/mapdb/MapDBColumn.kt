package ch.unibas.dmi.dbis.cottontail.database.column.mapdb

import ch.unibas.dmi.dbis.cottontail.database.column.*
import ch.unibas.dmi.dbis.cottontail.database.general.Transaction
import ch.unibas.dmi.dbis.cottontail.database.general.TransactionStatus
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema

import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.exceptions.TransactionException
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value

import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write

import org.mapdb.*
import org.mapdb.volume.MappedFileVol
import org.mapdb.volume.VolumeFactory

import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.StampedLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Represents a single column in the Cottontail DB model. A [MapDBColumn] record is identified by a tuple ID (long)
 * and can hold an arbitrary value. Usually, multiple [MapDBColumn]s make up an [Entity].
 *
 * @see Entity
 *
 * @param <T> Type of the value held by this [MapDBColumn].
 *
 * @author Ralph Gasser
 * @version 1.2
 */
class MapDBColumn<T : Value>(override val name: Name, override val parent: Entity) : Column<T> {
    /** Constant FQN of the [Schema] object. */
    override val fqn: Name = this.parent.fqn.append(this.name)

    /** The [Path] to the [Entity]'s main folder. */
    override val path: Path = parent.path.resolve("col_$name.db")

    /** Internal reference to the [Store] underpinning this [MapDBColumn]. */
    private var store: CottontailStoreWAL = try {
        CottontailStoreWAL.make(file = this.path.toString(), volumeFactory = this.parent.parent.parent.config.volumeFactory, fileLockWait = this.parent.parent.parent.config.lockTimeout)
    } catch (e: DBException) {
        throw DatabaseException("Failed to open column at '$path': ${e.message}'")
    }

    /** Internal reference to the [Header] of this [MapDBColumn]. */
    private val header
        get() = this.store.get(HEADER_RECORD_ID, ColumnHeaderSerializer)
                ?: throw DatabaseException.DataCorruptionException("Failed to open header of column '$fqn'!'")

    /**
     * Getter for this [MapDBColumn]'s [ColumnDef]. Can be stored since [MapDBColumn]s [ColumnDef] is immutable.
     *
     * @return [ColumnDef] for this [MapDBColumn]
     */
    @Suppress("UNCHECKED_CAST")
    override val columnDef: ColumnDef<T> = this.header.let { ColumnDef(this.fqn, it.type as ColumnType<T>, it.size, it.nullable) }

    /**
     * The maximum tuple ID used by this [Column].
     */
    override val maxTupleId: Long
        get() = this.store.maxRecid

    /**
     * Status indicating whether this [MapDBColumn] is open or closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    /** An internal lock that is used to synchronize concurrent read & write access to this [MapDBColumn] by different [MapDBColumn.Tx]. */
    private val txLock = StampedLock()

    /** An internal lock that is used to synchronize structural changes to an [MapDBColumn] (e.g. closing or deleting) with running [MapDBColumn.Tx]. */
    private val globalLock = StampedLock()

    /**
     * Closes the [MapDBColumn]. Closing an [MapDBColumn] is a delicate matter since ongoing [MapDBColumn.Tx]  are involved.
     * Therefore, access to the method is mediated by an global [MapDBColumn] wide lock.
     */
    override fun close() = this.globalLock.write {
        this.store.close()
        this.closed = true
    }

    /**
     * Creates a new [MapDBColumn.Tx] and returns it.
     *
     * @param readonly True, if the resulting [MapDBColumn.Tx] should be a read-only transaction.
     * @param tid The ID for the new [MapDBColumn.Tx]
     *
     * @return A new [ColumnTransaction] object.
     */
    override fun newTransaction(readonly: Boolean, tid: UUID): ColumnTransaction<T> = Tx(readonly, tid)

    /**
     * Companion object with some important constants.
     */
    companion object {
        /** Record ID of the [ColumnHeader]. */
        private const val HEADER_RECORD_ID: Long = 1L

        /**
         * Initializes a new, empty [MapDBColumn]
         *
         * @param parent The folder that contains the data file
         * @param definition The [ColumnDef] that specified the [MapDBColumn]
         * @param volumeFactory The [MappedFileVol.MappedFileFactory] used to initialize the [MapDBColumn]
         */
        fun initialize(definition: ColumnDef<*>, path: Path, volumeFactory: VolumeFactory) {
            val store = StoreWAL.make(file = path.resolve("col_${definition.name}.db").toString(), volumeFactory = volumeFactory)
            store.put(ColumnHeader(type = definition.type, size = definition.size, nullable = definition.nullable), ColumnHeaderSerializer)
            store.commit()
            store.close()
        }
    }

    /**
     * Thinly veiled implementation of the [Record] interface for internal use.
     */
    inner class ColumnRecord(override val tupleId: Long, val value: Value?) : Record {
        override val columns
            get() = arrayOf(this@MapDBColumn.columnDef)
        override val values
            get() = arrayOf(this.value)

        override fun first(): Value? = this.value
        override fun last(): Value? = this.value
        override fun copy(): Record = ColumnRecord(this.tupleId, this.value)
    }

    /**
     * A [Transaction] that affects this [MapDBColumn].
     */
    inner class Tx constructor(override val readonly: Boolean, override val tid: UUID) : ColumnTransaction<T> {

        /** Flag indicating whether or not this [Entity.Tx] was closed */
        @Volatile
        override var status: TransactionStatus = TransactionStatus.CLEAN
            private set

        /**
         * The [ColumnDef] of the [Column] underlying this [ColumnTransaction].
         *
         * @return [ColumnTransaction]
         */
        override val columnDef: ColumnDef<T>
            get() = this@MapDBColumn.columnDef

        /** Tries to acquire a global read-lock on the [MapDBColumn]. */
        init {
            if (this@MapDBColumn.closed) {
                throw TransactionException.TransactionDBOClosedException(tid)
            }
        }

        /** The [Serializer] used for de-/serialization of [MapDBColumn] entries. */
        private val serializer = this@MapDBColumn.type.serializer(this@MapDBColumn.columnDef.size)

        /** Obtains a global (non-exclusive) read-lock on [MapDBColumn]. Prevents enclosing [MapDBColumn] from being closed while this [MapDBColumn.Tx] is still in use. */
        private val globalStamp = this@MapDBColumn.globalLock.readLock()

        /** Obtains transaction lock on [MapDBColumn]. Prevents concurrent read & write access to the enclosing [MapDBColumn]. */
        private val txStamp = if (this.readonly) {
            this@MapDBColumn.txLock.readLock()
        } else {
            this@MapDBColumn.txLock.writeLock()
        }

        /** A [ReentrantReadWriteLock] local to this [Entity.Tx]. It makes sure, that this [Entity] cannot be committed, closed or rolled back while it is being used. */
        private val localLock = ReentrantReadWriteLock()

        /**
         * Commits all changes made through this [Tx] since the last commit or rollback.
         */
        @Synchronized
        override fun commit() = this.localLock.write {
            if (this.status == TransactionStatus.DIRTY) {
                this@MapDBColumn.store.commit()
                this.status = TransactionStatus.CLEAN
            }
        }

        /**
         * Rolls all changes made through this [Tx] back to the last commit. Can only be executed, if [Tx] is
         * in status [TransactionStatus.DIRTY] or [TransactionStatus.ERROR].
         */
        @Synchronized
        override fun rollback() = this.localLock.write {
            if (this.status == TransactionStatus.DIRTY || this.status == TransactionStatus.ERROR) {
                this@MapDBColumn.store.rollback()
                this.status = TransactionStatus.CLEAN
            }
        }

        /**
         * Closes this [Tx] and relinquishes the associated [ReentrantReadWriteLock].
         */
        @Synchronized
        override fun close() = this.localLock.write {
            if (this.status != TransactionStatus.CLOSED) {
                if (this.status == TransactionStatus.DIRTY || this.status == TransactionStatus.ERROR) {
                    this.rollback()
                }
                this.status = TransactionStatus.CLOSED
                this@MapDBColumn.txLock.unlock(this.txStamp)
                this@MapDBColumn.globalLock.unlockRead(this.globalStamp)
            }
        }

        /**
         * Gets and returns an entry from this [MapDBColumn].
         *
         * @param tupleId The ID of the desired entry
         * @return The desired entry.
         *
         * @throws DatabaseException If the tuple with the desired ID doesn't exist OR is invalid.
         */
        override fun read(tupleId: Long): T? = this.localLock.read {
            checkValidForRead()
            checkValidTupleId(tupleId)
            return this@MapDBColumn.store.get(tupleId, this.serializer)
        }

        /**
         * Gets and returns several entries from this [MapDBColumn].
         *
         * @param tupleIds The IDs of the desired entries
         * @return List of the desired entries.
         *
         * @throws DatabaseException If the tuple with the desired ID doesn't exist OR is invalid.
         */
        override fun readAll(tupleIds: Collection<Long>): Collection<T?> = this.localLock.read {
            checkValidForRead()
            return tupleIds.map {
                checkValidTupleId(it)
                this@MapDBColumn.store.get(it, this.serializer)
            }
        }

        /**
         * Returns the number of entries in this [MapDBColumn]. Action acquires a global read dataLock for the [MapDBColumn].
         *
         * @return The number of entries in this [MapDBColumn].
         */
        override fun count(): Long = this.localLock.read {
            checkValidForRead()
            return this@MapDBColumn.header.count
        }

        /**
         * Applies the provided function on each element found in this [MapDBColumn]. The function cannot not change
         * the data stored in the [MapDBColumn]!
         *
         * @param action The function that should be applied.
         */
        override fun forEach(action: (Record) -> Unit) = forEach(1L, this@MapDBColumn.store.maxRecid, action)

        /**
         * Applies the provided function on each element found in the given range in this [MapDBColumn]. The function
         * cannot not change the data stored in the [MapDBColumn]!
         *
         * @param from The tuple ID of the first [Record] to iterate over.
         * @param to The tuple ID of the last [Record] to iterate over.
         * @param action The function that should be applied.
         */
        override fun forEach(from: Long, to: Long, action: (Record) -> Unit) = this.localLock.read {
            checkValidForRead()
            this@MapDBColumn.store.RecordIdIterator(from.coerceAtLeast(1L), to.coerceAtMost(this@MapDBColumn.store.maxRecid)).use { iterator ->
                iterator.forEachRemaining {
                    if (it != CottontailStoreWAL.EOF_ENTRY) {
                        action(ColumnRecord(it, this@MapDBColumn.store.get(it, this.serializer)))
                    }
                }
            }
        }

        /**
         * Applies the provided mapping function on each value found in the given range in this [MapDBColumn],
         * returning a collection of the desired output values.
         *
         * @param action The mapping function that should be applied.
         * @return A collection of Pairs mapping the tupleId to the generated value.
         */
        override fun <R> map(action: (Record) -> R): Collection<R> = map(1L, this@MapDBColumn.store.maxRecid, action)

        /**
         * Applies the provided mapping function on each value found in this [MapDBColumn], returning a collection
         * of the desired output values.
         *
         * @param from The tuple ID of the first [Record] to iterate over.
         * @param to The tuple ID of the last [Record] to iterate over.
         * @param action The mapping function that should be applied.
         * @return A collection of Pairs mapping the tupleId to the generated value.
         */
        override fun <R> map(from: Long, to: Long, action: (Record) -> R): Collection<R> = this.localLock.read {
            checkValidForRead()
            val list = mutableListOf<R>()
            this@MapDBColumn.store.RecordIdIterator(from.coerceAtLeast(1L), to.coerceAtMost(this@MapDBColumn.store.maxRecid)).use { iterator ->
                iterator.forEachRemaining {
                    if (it != CottontailStoreWAL.EOF_ENTRY) {
                        list.add(action(ColumnRecord(it, this@MapDBColumn.store.get(it, this.serializer))))
                    }
                }
            }
            return list
        }


        /**
         * Checks whether or not this [MapDBColumn] can process the given predicate and returns true or false respectively.
         *
         * @param predicate The [BooleanPredicate] to check.
         * @return True if predicate can be processed, false otherwise.
         */
        override fun canProcess(predicate: Predicate): Boolean = predicate is BooleanPredicate

        /**
         * Applies the provided predicate to each [Record] found in this [MapDBColumn], returning a [Recordset] that contains all
         * output values that pass the predicate's test (i.e. return true)
         *
         * @param predicate The [BooleanPredicate] that should be applied.
         * @return A filtered [Recordset] of [Record]s that passed the test.
         */
        override fun filter(predicate: Predicate): Recordset = this.localLock.read {
            if (predicate is BooleanPredicate) {
                checkValidForRead()
                val recordset = Recordset(arrayOf(this@MapDBColumn.columnDef))
                this@MapDBColumn.store.RecordIdIterator().use { iterator ->
                    iterator.forEachRemaining {
                        if (it != CottontailStoreWAL.EOF_ENTRY) {
                            val data = ColumnRecord(it, this@MapDBColumn.store.get(it, this.serializer))
                            if (predicate.matches(data)) recordset.addRowUnsafe(data.values)
                        }
                    }
                }
                return recordset
            } else {
                throw QueryException.UnsupportedPredicateException("MapDBColumn#filter() does not support predicates of type '${predicate::class.simpleName}'.")
            }
        }

        /**
         * Applies the provided action to each [Record] that matches the given [Predicate]. The function cannot not change
         * the data stored in the [MapDBColumn]!
         *
         * @param predicate The [BooleanPredicate] to filter [Record]s.
         * @param action The function that should be applied.
         *
         * @throws QueryException.UnsupportedPredicateException If predicate is not a [BooleanPredicate].
         */
        override fun forEach(predicate: Predicate, action: (Record) -> Unit) = forEach(1L, this@MapDBColumn.store.maxRecid, predicate, action)

        /**
         * Applies the provided action to each [Record] in the given range that matches the given [Predicate]. The function cannot not change
         * the data stored in the [MapDBColumn]!
         *
         * @param from The tuple ID of the first [Record] to iterate over.
         * @param to The tuple ID of the last [Record] to iterate over.
         * @param predicate The [BooleanPredicate] to filter [Record]s.
         * @param action The function that should be applied.
         *
         * @throws QueryException.UnsupportedPredicateException If predicate is not a [BooleanPredicate].
         */
        override fun forEach(from: Long, to: Long, predicate: Predicate, action: (Record) -> Unit) = this.localLock.read {
            if (predicate is BooleanPredicate) {
                checkValidForRead()
                this@MapDBColumn.store.RecordIdIterator(from.coerceAtLeast(1L), to.coerceAtMost(this@MapDBColumn.store.maxRecid)).use { iterator ->
                    iterator.forEachRemaining {
                        if (it != CottontailStoreWAL.EOF_ENTRY) {
                            val record = ColumnRecord(it, this@MapDBColumn.store.get(it, this.serializer))
                            if (predicate.matches(record)) {
                                action(record)
                            }
                        }
                    }
                }
            } else {
                throw QueryException.UnsupportedPredicateException("MapDBColumn#forEach() does not support predicates of type '${predicate::class.simpleName}'.")
            }
        }

        /**
         * Applies the provided mapping function to each [Record] that matches the given [Predicate], returning a collection
         * of the desired output values.
         *
         * @param predicate The [Predicate] to filter [Record]s.
         * @param action The mapping function that should be applied.
         * @return Collection of the results of the mapping function.
         *
         * @throws QueryException.UnsupportedPredicateException If predicate is not a [BooleanPredicate].
         */
        override fun <R> map(predicate: Predicate, action: (Record) -> R): Collection<R> = map(1L, this@MapDBColumn.store.maxRecid, predicate, action)

        /**
         * Applies the provided mapping function to each [Record] in the given range that matches the given [Predicate], returning a collection
         * of the desired output values.
         *
         * @param from The tuple ID of the first [Record] to iterate over.
         * @param to The tuple ID of the last [Record] to iterate over.
         * @param predicate The [BooleanPredicate] to filter [Record]s.
         * @param action The mapping function that should be applied.
         * @return Collection of the results of the mapping function.
         *
         * @throws QueryException.UnsupportedPredicateException If predicate is not a [BooleanPredicate].
         */
        override fun <R> map(from: Long, to: Long, predicate: Predicate, action: (Record) -> R): Collection<R> = this.localLock.read {
            if (predicate is BooleanPredicate) {
                checkValidForRead()
                val list = mutableListOf<R>()
                this@MapDBColumn.store.RecordIdIterator(from.coerceAtLeast(1L), to.coerceAtMost(this@MapDBColumn.store.maxRecid)).use { iterator ->
                    iterator.forEachRemaining {
                        if (it != CottontailStoreWAL.EOF_ENTRY) {
                            val record = ColumnRecord(it, this@MapDBColumn.store.get(it, this.serializer))
                            if (predicate.matches(record)) {
                                list.add(action(ColumnRecord(it, this@MapDBColumn.store.get(it, this.serializer))))
                            }
                        }
                    }
                }
                return list
            } else {
                throw QueryException.UnsupportedPredicateException("MapDBColumn#map() does not support predicates of type '${predicate::class.simpleName}'.")
            }
        }

        /**
         * Inserts a new record in this [MapDBColumn]. This tasks will set this [MapDBColumn.Tx] to [TransactionStatus.DIRTY]
         * and acquire a column-wide write lock until the [MapDBColumn.Tx] either commit or rollback is issued.
         *
         * @param record The record that should be inserted. Can be null!
         * @return The tupleId of the inserted record OR the allocated space in case of a null value.
         */
        override fun insert(record: T?): Long = this.localLock.read {
            try {
                checkValidForWrite()
                val tupleId = if (record == null) {
                    this@MapDBColumn.store.preallocate()
                } else {
                    this@MapDBColumn.store.put(this@MapDBColumn.type.cast(record), this.serializer)
                }

                /* Update header. */
                val header = this@MapDBColumn.header
                header.count += 1
                header.modified = System.currentTimeMillis()
                this@MapDBColumn.store.update(HEADER_RECORD_ID, header, ColumnHeaderSerializer)
                tupleId
            } catch (e: DBException) {
                this.status = TransactionStatus.ERROR
                throw TransactionException.TransactionStorageException(this.tid, e.message ?: "Unknown")
            }
        }

        /**
         * Inserts a list of new records in this [MapDBColumn]. This tasks will set this [MapDBColumn.Tx] to [TransactionStatus.DIRTY]
         * and acquire a column-wide write lock until the [MapDBColumn.Tx] either commit or rollback is issued.
         *
         * @param records The records that should be inserted. Can contain null values!
         * @return The tupleId of the inserted record OR the allocated space in case of a null value.
         */
        override fun insertAll(records: Collection<T?>): Collection<Long> = this.localLock.read {
            try {
                checkValidForWrite()

                val tupleIds = records.map {
                    if (it == null) {
                        this@MapDBColumn.store.preallocate()
                    } else {
                        this@MapDBColumn.store.put(this@MapDBColumn.type.cast(it), serializer)
                    }
                }

                /* Update header. */
                val header = this@MapDBColumn.header
                header.count += records.size
                header.modified = System.currentTimeMillis()
                this@MapDBColumn.store.update(HEADER_RECORD_ID, header, ColumnHeaderSerializer)
                tupleIds
            } catch (e: DBException) {
                this.status = TransactionStatus.ERROR
                throw TransactionException.TransactionStorageException(this.tid, e.message ?: "Unknown")
            }
        }

        /**
         * Updates the entry with the specified tuple ID and sets it to the new value. This tasks will set this [MapDBColumn.Tx]
         * to [TransactionStatus.DIRTY] and acquire a column-wide write lock until the [MapDBColumn.Tx] either commit or rollback is issued.
         *
         * @param tupleId The ID of the record that should be updated
         * @param value The new value.
         */
        override fun update(tupleId: Long, value: T?) = this.localLock.read {
            try {
                checkValidForWrite()
                checkValidTupleId(tupleId)
                this@MapDBColumn.store.update(tupleId, value, this.serializer)
            } catch (e: DBException) {
                this.status = TransactionStatus.ERROR
                throw TransactionException.TransactionStorageException(this.tid, e.message ?: "Unknown")
            }
        }

        /**
         * Updates the entry with the specified tuple ID and sets it to the new value. This tasks will set this [MapDBColumn.Tx]
         * to [TransactionStatus.DIRTY] and acquire a column-wide write lock until the [MapDBColumn.Tx] either commit or rollback is issued.
         *
         * @param tupleId The ID of the record that should be updated
         * @param value The new value.
         * @param expected The value expected to be there.
         */
        override fun compareAndUpdate(tupleId: Long, value: T?, expected: T?): Boolean = this.localLock.read {
            try {
                checkValidForWrite()
                checkValidTupleId(tupleId)
                this@MapDBColumn.store.compareAndSwap(tupleId, expected, value, this.serializer)
            } catch (e: DBException) {
                this.status = TransactionStatus.ERROR
                throw TransactionException.TransactionStorageException(this.tid, e.message ?: "Unknown")
            }
        }

        /**
         * Deletes a record from this [MapDBColumn]. This tasks will set this [MapDBColumn.Tx] to [TransactionStatus.DIRTY]
         * and acquire a column-wide write lock until the [MapDBColumn.Tx] either commit or rollback is issued.
         *
         * @param tupleId The ID of the record that should be deleted
         */
        override fun delete(tupleId: Long) = this.localLock.read {
            try {
                checkValidForWrite()
                checkValidTupleId(tupleId)
                this@MapDBColumn.store.delete(tupleId, this.serializer)

                /* Update header. */
                val header = this@MapDBColumn.header
                header.count -= 1
                header.modified = System.currentTimeMillis()
                this@MapDBColumn.store.update(HEADER_RECORD_ID, header, ColumnHeaderSerializer)
            } catch (e: DBException) {
                this.status = TransactionStatus.ERROR
                throw TransactionException.TransactionStorageException(this.tid, e.message ?: "Unknown")
            }
        }

        /**
         * Deletes all the specified records from this [MapDBColumn]. This tasks will set this [MapDBColumn.Tx] to [TransactionStatus.DIRTY]
         * and acquire a column-wide write lock until the [MapDBColumn.Tx] either commit or rollback is issued.
         *
         * @param tupleIds The IDs of the records that should be deleted.
         */
        override fun deleteAll(tupleIds: Collection<Long>) = this.localLock.read {
            try {
                checkValidForWrite()
                tupleIds.forEach {
                    checkValidTupleId(it)
                    this@MapDBColumn.store.delete(it, this.serializer)
                }

                /* Update header. */
                val header = this@MapDBColumn.header
                header.count -= tupleIds.size
                header.modified = System.currentTimeMillis()
                store.update(HEADER_RECORD_ID, header, ColumnHeaderSerializer)
            } catch (e: DBException) {
                this.status = TransactionStatus.ERROR
                throw TransactionException.TransactionStorageException(this.tid, e.message ?: "Unknown")
            }
        }

        /**
         * Checks if the provided tupleID is valid. Otherwise, an exception will be thrown.
         */
        private fun checkValidTupleId(tupleId: Long) {
            if ((tupleId < 0L) or (tupleId == HEADER_RECORD_ID)) {
                throw TransactionException.InvalidTupleId(tid, tupleId)
            }
        }

        /**
         * Checks if this [MapDBColumn.Tx] is still open. Otherwise, an exception will be thrown.
         */
        @Synchronized
        private fun checkValidForRead() {
            if (this.status == TransactionStatus.CLOSED) throw TransactionException.TransactionClosedException(tid)
            if (this.status == TransactionStatus.ERROR) throw TransactionException.TransactionInErrorException(tid)
        }

        /**
         * Tries to acquire a write-lock. If method fails, an exception will be thrown
         */
        @Synchronized
        private fun checkValidForWrite() {
            if (this.readonly) throw TransactionException.TransactionReadOnlyException(tid)
            if (this.status == TransactionStatus.CLOSED) throw TransactionException.TransactionClosedException(tid)
            if (this.status == TransactionStatus.ERROR) throw TransactionException.TransactionInErrorException(tid)
            if (this.status != TransactionStatus.DIRTY) {
                this.status = TransactionStatus.DIRTY
            }
        }
    }
}