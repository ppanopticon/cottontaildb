package org.vitrivr.cottontail.database.column.mapdb

import org.mapdb.*
import org.vitrivr.cottontail.config.MapDBConfig
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.column.ColumnTx
import org.vitrivr.cottontail.database.column.Type
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.general.TxStatus
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.TxException
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.StampedLock

/**
 * Represents a [Column] in the Cottontail DB model that uses the Map DB storage engine.
 *
 * @see Entity
 *
 * @param <T> Type of the value held by this [MapDBColumn].
 *
 * @author Ralph Gasser
 * @version 1.4.2
 */
class MapDBColumn<T : Value>(override val name: Name.ColumnName, override val parent: Entity) : Column<T> {
    /**
     * Companion object with some important constants.
     */
    companion object {
        /** Record ID of the [ColumnHeader]. */
        private const val HEADER_RECORD_ID: Long = 1L

        /**
         * Initializes a new, empty [MapDBColumn]
         *
         * @param definition The [ColumnDef] that specified the [MapDBColumn]
         * @param config The [MapDBConfig] used to initialize the [MapDBColumn]
         */
        fun initialize(definition: ColumnDef<*>, path: Path, config: MapDBConfig) {
            val store = StoreWAL.make(
                    file = path.resolve("col_${definition.name.simple}.db").toString(),
                    volumeFactory = config.volumeFactory,
                    allocateIncrement = 1L shl config.pageShift
            )
            store.put(
                ColumnHeader(type = definition.type, nullable = definition.nullable),
                ColumnHeader.Serializer
            )
            store.commit()
            store.close()
        }
    }

    /** The [Path] to the [Entity]'s main folder. */
    override val path: Path = parent.path.resolve("col_${name.simple}.db")

    /** Internal reference to the [Store] underpinning this [MapDBColumn]. */
    private var store: CottontailStoreWAL = try {
        this.parent.parent.parent.config.mapdb.store(this.path)
    } catch (e: DBException) {
        throw DatabaseException("Failed to open column at '$path': ${e.message}'")
    }

    /** Internal reference to the header of this [MapDBColumn]. */
    private val header
        get() = this.store.get(HEADER_RECORD_ID, ColumnHeader.Serializer)
            ?: throw DatabaseException.DataCorruptionException("Failed to open header of column '$name'!'")

    /**
     * Getter for this [MapDBColumn]'s [ColumnDef]. Can be stored since [MapDBColumn]s [ColumnDef] is immutable.
     *
     * @return [ColumnDef] for this [MapDBColumn]
     */
    @Suppress("UNCHECKED_CAST")
    override val columnDef: ColumnDef<T> =
        this.header.let { ColumnDef(this.name, it.type as Type<T>, it.nullable) }

    /**
     * Status indicating whether this [MapDBColumn] is open or closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

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
     * Creates and returns a new [MapDBColumn.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [MapDBColumn.Tx] for.
     * @return New [MapDBColumn.Tx]
     */
    override fun newTx(context: TransactionContext) = Tx(context)

    /**
     * A [Tx] that affects this [MapDBColumn].
     *
     * @author Ralph Gasser
     * @version 1.4.0
     */
    inner class Tx constructor(context: TransactionContext) : AbstractTx(context), ColumnTx<T> {

        /** Reference to the [MapDBColumn] this [MapDBColumn.Tx] belongs to. */
        override val dbo: Column<T>
            get() = this@MapDBColumn

        /** Tries to acquire a global read-lock on the surrounding column. */
        init {
            if (this@MapDBColumn.closed) {
                throw TxException.TxDBOClosedException(this.context.txId)
            }
        }

        /** The [Serializer] used for de-/serialization of [MapDBColumn] entries. */
        private val serializer = this@MapDBColumn.type.serializer()

        /** Obtains a global (non-exclusive) read-lock on [MapDBColumn]. Prevents enclosing [MapDBColumn] from being closed while this [MapDBColumn.Tx] is still in use. */
        private val globalStamp = this@MapDBColumn.globalLock.readLock()

        /** In-memory snapshot of the [MapDBColumn]'s header (evaluated lazily, since not always needed). */
        private val header: ColumnHeader by lazy { this@MapDBColumn.header }

        /**
         * Gets and returns an entry from this [MapDBColumn].
         *
         * @param tupleId The ID of the desired entry
         * @return The desired entry.
         *
         * @throws DatabaseException If the tuple with the desired ID doesn't exist OR is invalid.
         */
        override fun read(tupleId: Long): T? = this.withReadLock {
            return this@MapDBColumn.store.get(tupleId, this.serializer)
        }

        /**
         * Returns the number of entries in this [MapDBColumn]. Action acquires a global read dataLock for the [MapDBColumn].
         *
         * @return The number of entries in this [MapDBColumn].
         */
        override fun count(): Long = this.withReadLock {
            return this.header.count
        }

        /**
         * Returns the maximum [TupleId] known by this [MapDBColumn].
         */
        override fun maxTupleId(): TupleId = this.withReadLock {
            this@MapDBColumn.store.maxRecid
        }

        /**
         * Creates and returns a new [CloseableIterator] for this [MapDBColumn.Tx] that returns
         * all [TupleId]s contained within the surrounding [MapDBColumn].
         *
         * @return [CloseableIterator]
         */
        override fun scan() = this.scan(1L..this.maxTupleId())

        /**
         * Creates and returns a new [CloseableIterator] for this [MapDBColumn.Tx] that returns
         * all [TupleId]s contained within the surrounding [MapDBColumn] and a certain range.
         *
         * @param range The [LongRange] that should be scanned.
         * @return [CloseableIterator]
         */
        override fun scan(range: LongRange) = object : CloseableIterator<TupleId> {

            init {
                this@Tx.withReadLock { /* No op. */ }
            }

            /** Wraps a [CottontailStoreWAL.RecordIdIterator] from the [MapDBColumn]. */
            private val wrapped = this@MapDBColumn.store.RecordIdIterator(range)

            /** Flag indicating whether this [CloseableIterator] has been closed. */
            @Volatile
            override var isOpen = true
                private set

            /**
             * Returns the next element in the iteration.
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
             * Closes this [CloseableIterator] and releases all locks associated with it.
             */
            override fun close() {
                if (this.isOpen) {
                    this.isOpen = true
                }
            }
        }

        /**
         * Inserts a new record in this [MapDBColumn]. This tasks will set this [MapDBColumn.Tx] to
         * [TxStatus.DIRTY] and acquire a column-wide write lock until the [MapDBColumn.Tx] either
         * commit or rollback is issued.
         *
         * @param record The record that should be inserted. Can be null!
         * @return The tupleId of the inserted record OR the allocated space in case of a null value.
         */
        override fun insert(record: T?): Long = this.withWriteLock {
            try {
                val tupleId = if (record == null && this.columnDef.nullable) {
                    this@MapDBColumn.store.preallocate()
                } else if (record != null) {
                    this@MapDBColumn.store.put(record, this.serializer)
                } else {
                    throw IllegalArgumentException("Column $columnDef does not allow for NULL values.")
                }

                /* Update header. */
                this.header.count += 1
                return tupleId
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw TxException.TxStorageException(this.context.txId, e.message ?: "Unknown")
            }
        }

        /**
         * Updates the entry with the specified tuple ID and sets it to the new value.
         *
         * This will set this [MapDBColumn.Tx] to [TxStatus.DIRTY] and acquire a column-wide
         * write lock until the [MapDBColumn.Tx] either commit or rollback is issued.
         *
         * @param tupleId The ID of the record that should be updated
         * @param value The new [Value] or null.
         * @return The old [Value] or null
         */
        override fun update(tupleId: TupleId, value: T?): T? = this.withWriteLock {
            try {
                val ret = this@MapDBColumn.store.get(tupleId, this.serializer)
                this@MapDBColumn.store.update(tupleId, value, this.serializer)
                return ret
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw TxException.TxStorageException(
                    this.context.txId,
                    e.message ?: "Unknown exception during data storage"
                )
            }
        }

        /**
         * Updates the entry with the specified tuple ID and sets it to the new value.
         *
         * This will set this [MapDBColumn.Tx] to [TxStatus.DIRTY] and acquire a column-wide
         * write lock until the [MapDBColumn.Tx] either commit or rollback is issued.
         *
         * @param tupleId The ID of the record that should be updated
         * @param value The new [Value].
         * @param expected The [Value] expected to be there.
         * @return True upon success, false otherwise.
         */
        override fun compareAndUpdate(tupleId: TupleId, value: T?, expected: T?): Boolean =
            this.withWriteLock {
                try {
                    return this@MapDBColumn.store.compareAndSwap(
                        tupleId,
                        expected,
                        value,
                        this.serializer
                    )
                } catch (e: DBException) {
                    this.status = TxStatus.ERROR
                    throw TxException.TxStorageException(this.context.txId, e.message ?: "Unknown")
                }
            }

        /**
         * Deletes a record from this [MapDBColumn].
         *
         * This tasks will set this [MapDBColumn.Tx] to [TxStatus.DIRTY] and acquire a column-wide
         * write lock until the [MapDBColumn.Tx] either COMMIT or ROLLBACK is issued.
         *
         * @param tupleId The [TupleId] of the record that should be deleted
         * @return The old [Value]
         */
        override fun delete(tupleId: TupleId): T? = this.withWriteLock {
            try {
                val ret = this@MapDBColumn.store.get(tupleId, this.serializer)
                this@MapDBColumn.store.delete(tupleId, this.serializer)

                /* Update header. */
                this.header.count -= 1
                this@MapDBColumn.store.update(HEADER_RECORD_ID, header, ColumnHeader.Serializer)

                return ret
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw TxException.TxStorageException(this.context.txId, e.message ?: "Unknown")
            }
        }

        /**
         * Performs a COMMIT of all changes made through this [MapDBColumn.Tx].
         */
        override fun performCommit() {
            /** Update and persist header + commit store. */
            this.header.modified = System.currentTimeMillis()
            this@MapDBColumn.store.update(HEADER_RECORD_ID, this.header, ColumnHeader.Serializer)
            this@MapDBColumn.store.commit()
        }

        /**
         * Performs a ROLLBACK of all changes made through this [MapDBColumn.Tx].
         */
        override fun performRollback() {
            this@MapDBColumn.store.rollback()
        }

        /**
         * Releases the [globalLock] on the [MapDBColumn].
         */
        override fun cleanup() {
            this@MapDBColumn.globalLock.unlockRead(this.globalStamp)
        }
    }
}