package org.vitrivr.cottontail.database.column.mapdb

import org.mapdb.*
import org.vitrivr.cottontail.config.MemoryConfig
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.column.ColumnCursor
import org.vitrivr.cottontail.database.column.ColumnTransaction
import org.vitrivr.cottontail.database.column.ColumnType
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.Transaction
import org.vitrivr.cottontail.database.general.TransactionStatus
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.TransactionException
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.utilities.extensions.exclusive
import org.vitrivr.cottontail.utilities.extensions.shared
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.StampedLock

/**
 * Represents a single column in the Cottontail DB model. A [MapDBColumn] record is identified by a tuple ID (long)
 * and can hold an arbitrary value. Usually, multiple [MapDBColumn]s make up an [Entity].
 *
 * @see Entity
 *
 * @param <T> Type of the value held by this [MapDBColumn].
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class MapDBColumn<T : Value>(override val name: Name.ColumnName, override val parent: Entity) : Column<T> {

    /** The [Path] to the [Entity]'s main folder. */
    override val path: Path = parent.path.resolve("col_${name.simple}.db")

    /** Internal reference to the [Store] underpinning this [MapDBColumn]. */
    private var store: CottontailStoreWAL = try {
        CottontailStoreWAL.make(
                file = this.path.toString(),
                volumeFactory = this.parent.parent.parent.config.memoryConfig.volumeFactory,
                allocateIncrement = (1L shl this.parent.parent.parent.config.memoryConfig.dataPageShift),
                fileLockWait = this.parent.parent.parent.config.lockTimeout
        )
    } catch (e: DBException) {
        throw DatabaseException("Failed to open column at '$path': ${e.message}'")
    }

    /** Internal reference to the [Header] of this [MapDBColumn]. */
    private val header
        get() = this.store.get(HEADER_RECORD_ID, ColumnHeaderSerializer)
                ?: throw DatabaseException.DataCorruptionException("Failed to open header of column '$name'!'")

    /**
     * Getter for this [MapDBColumn]'s [ColumnDef]. Can be stored since [MapDBColumn]s [ColumnDef] is immutable.
     *
     * @return [ColumnDef] for this [MapDBColumn]
     */
    @Suppress("UNCHECKED_CAST")
    override val columnDef: ColumnDef<T> = this.header.let { ColumnDef(this.name, it.type as ColumnType<T>, it.size, it.nullable) }

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
         * @param config The [MemoryConfig] used to initialize the [MapDBColumn]
         */
        fun initialize(definition: ColumnDef<*>, path: Path, config: MemoryConfig) {
            val store = StoreWAL.make(
                    file = path.resolve("col_${definition.name.simple}.db").toString(),
                    volumeFactory = config.volumeFactory,
                    allocateIncrement = 1L shl config.dataPageShift
            )
            store.put(ColumnHeader(type = definition.type, size = definition.logicalSize, nullable = definition.nullable), ColumnHeaderSerializer)
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
        private val serializer = this@MapDBColumn.type.serializer(this@MapDBColumn.columnDef.logicalSize)

        /** Obtains a global (non-exclusive) read-lock on [MapDBColumn]. Prevents enclosing [MapDBColumn] from being closed while this [MapDBColumn.Tx] is still in use. */
        private val globalStamp = this@MapDBColumn.globalLock.readLock()

        /** Obtains transaction lock on [MapDBColumn]. Prevents concurrent read & write access to the enclosing [MapDBColumn]. */
        private val txStamp = if (this.readonly) {
            this@MapDBColumn.txLock.readLock()
        } else {
            this@MapDBColumn.txLock.writeLock()
        }

        /** A [ReentrantReadWriteLock] local to this [Entity.Tx]. It makes sure, that this [Entity] cannot be committed, closed or rolled back while it is being used. */
        private val localLock = StampedLock()

        /**
         * Commits all changes made through this [Tx] since the last commit or rollback.
         */
        override fun commit() = this.localLock.exclusive {
            if (this.status == TransactionStatus.DIRTY) {
                this@MapDBColumn.store.commit()
                this.status = TransactionStatus.CLEAN
            }
        }

        /**
         * Rolls all changes made through this [Tx] back to the last commit. Can only be executed, if [Tx] is
         * in status [TransactionStatus.DIRTY] or [TransactionStatus.ERROR].
         */
        override fun rollback() = this.localLock.exclusive {
            if (this.status == TransactionStatus.DIRTY || this.status == TransactionStatus.ERROR) {
                this@MapDBColumn.store.rollback()
                this.status = TransactionStatus.CLEAN
            }
        }

        /**
         * Closes this [Tx] and relinquishes the associated [ReentrantReadWriteLock].
         */
        override fun close() = this.localLock.exclusive {
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
        override fun read(tupleId: Long): T? = this.localLock.shared {
            checkValidForRead()
            return this@MapDBColumn.store.get(tupleId, this.serializer)
        }

        /**
         * Returns the number of entries in this [MapDBColumn]. Action acquires a global read dataLock for the [MapDBColumn].
         *
         * @return The number of entries in this [MapDBColumn].
         */
        override fun count(): Long = this.localLock.shared {
            checkValidForRead()
            return this@MapDBColumn.header.count
        }

        /**
         * Returns the maximum [TupleId] for this [MapDBColumn].
         *
         * @return The maximum [TupleId] for this [MapDBColumn].
         */
        override fun maxTupleId(): TupleId = this.localLock.shared {
            checkValidForRead()
            return this@MapDBColumn.store.maxRecid
        }

        /**
         * Creates and returns a new [ColumnCursor] for this [MapDBColumn.Tx] that returns all
         * [TupleId]s contained within the surrounding [MapDBColumn].
         *
         * @return [ColumnCursor]
         */
        override fun scan() = this.scan(1L..this.maxTupleId())

        /**
         * Creates and returns a new [ColumnCursor] for this [MapDBColumn.Tx] that returns
         * all [TupleId]s contained within the surrounding [MapDBColumn] and a certain range.
         *
         * @param range The [LongRange] that should be scanned.
         * @return [ColumnCursor]
         */
        override fun scan(range: LongRange) = object : ColumnCursor<T> {

            init {
                checkValidForRead()
            }

            /** Acquires a read lock on the surrounding [MapDBColumn.Tx]*/
            private val lock = this@Tx.localLock.readLock()

            /** Wraps a [RecordIdIterator] from the [MapDBColumn]. */
            private val wrapped = this@MapDBColumn.store.RecordIdIterator(range)

            /** Flag indicating whether this [ColumnCursor] has been closed. */
            @Volatile
            override var isOpen = true
                private set

            /**
             * Returns the next [TupleId] in the iteration.
             */
            override fun next(): TupleId {
                check(this.isOpen) { "Illegal invocation of next(): This CloseableIterator has been closed." }
                return this.wrapped.next()
            }

            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean {
                check(this.isOpen) { "Illegal invocation of hasNext(): This CloseableIterator has been closed." }
                return this.wrapped.hasNext()
            }

            /**
             * Reads the value at the current [ColumnCursor] position and returns it.
             *
             * @return The value [T] at the position of this [ColumnCursor].
             */
            override fun readThrough(): T? {
                return this@MapDBColumn.store.get(this.next(), this@Tx.serializer)
            }

            /**
             * Closes this [ColumnCursor] and releases all locks associated with it.
             */
            override fun close() {
                if (this.isOpen) {
                    this@Tx.localLock.unlock(this.lock)
                    this.isOpen = false
                }
            }
        }

        /**
         * Inserts a new record in this [MapDBColumn]. This tasks will set this [MapDBColumn.Tx] to [TransactionStatus.DIRTY]
         * and acquire a column-wide write lock until the [MapDBColumn.Tx] either commit or rollback is issued.
         *
         * @param record The record that should be inserted. Can be null!
         * @return The tupleId of the inserted record OR the allocated space in case of a null value.
         */
        override fun insert(record: T?): Long = this.localLock.shared {
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
                throw TransactionException.TransactionStorageException(this.tid, e.message
                        ?: "Unknown")
            }
        }

        /**
         * Updates the entry with the specified tuple ID and sets it to the new value. This tasks will set this [MapDBColumn.Tx]
         * to [TransactionStatus.DIRTY] and acquire a column-wide write lock until the [MapDBColumn.Tx] either commit or rollback is issued.
         *
         * @param tupleId The ID of the record that should be updated
         * @param value The new value.
         */
        override fun update(tupleId: Long, value: T?) = this.localLock.shared {
            try {
                checkValidForWrite()
                this@MapDBColumn.store.update(tupleId, value, this.serializer)
            } catch (e: DBException) {
                this.status = TransactionStatus.ERROR
                throw TransactionException.TransactionStorageException(this.tid, e.message
                        ?: "Unknown")
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
        override fun compareAndUpdate(tupleId: Long, value: T?, expected: T?): Boolean = this.localLock.shared {
            try {
                checkValidForWrite()
                this@MapDBColumn.store.compareAndSwap(tupleId, expected, value, this.serializer)
            } catch (e: DBException) {
                this.status = TransactionStatus.ERROR
                throw TransactionException.TransactionStorageException(this.tid, e.message
                        ?: "Unknown")
            }
        }

        /**
         * Deletes a record from this [MapDBColumn]. This tasks will set this [MapDBColumn.Tx] to [TransactionStatus.DIRTY]
         * and acquire a column-wide write lock until the [MapDBColumn.Tx] either commit or rollback is issued.
         *
         * @param tupleId The ID of the record that should be deleted
         */
        override fun delete(tupleId: Long) = this.localLock.shared {
            try {
                checkValidForWrite()
                this@MapDBColumn.store.delete(tupleId, this.serializer)

                /* Update header. */
                val header = this@MapDBColumn.header
                header.count -= 1
                header.modified = System.currentTimeMillis()
                this@MapDBColumn.store.update(HEADER_RECORD_ID, header, ColumnHeaderSerializer)
            } catch (e: DBException) {
                this.status = TransactionStatus.ERROR
                throw TransactionException.TransactionStorageException(this.tid, e.message
                        ?: "Unknown")
            }
        }

        /**
         * Checks if this [MapDBColumn.Tx] is still open. Otherwise, an exception will be thrown.
         */
        private fun checkValidForRead() {
            if (this.status == TransactionStatus.CLOSED) throw TransactionException.TransactionClosedException(this.tid)
            if (this.status == TransactionStatus.ERROR) throw TransactionException.TransactionInErrorException(this.tid)
        }

        /**
         * Tries to acquire a write-lock. If method fails, an exception will be thrown
         */
        private fun checkValidForWrite() {
            if (this.readonly) throw TransactionException.TransactionReadOnlyException(this.tid)
            if (this.status == TransactionStatus.CLOSED) throw TransactionException.TransactionClosedException(this.tid)
            if (this.status == TransactionStatus.ERROR) throw TransactionException.TransactionInErrorException(this.tid)
            if (this.status != TransactionStatus.DIRTY) {
                this.status = TransactionStatus.DIRTY
            }
        }
    }
}