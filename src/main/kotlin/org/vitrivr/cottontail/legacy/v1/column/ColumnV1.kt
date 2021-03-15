package org.vitrivr.cottontail.legacy.v1.column

import org.mapdb.CottontailStoreWAL
import org.mapdb.DBException
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnEngine
import org.vitrivr.cottontail.database.column.ColumnTx
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.database.general.TxStatus
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.StampedLock

/**
 * Represents a single column in the Cottontail DB model. A [ColumnV1] record is identified by a tuple ID (long)
 * and can hold an arbitrary value. Usually, multiple [ColumnV1]s make up an [Entity].
 *
 * @see Entity
 *
 * @param <T> Type of the value held by this [ColumnV1].
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class ColumnV1<T : Value>(
    override val name: Name.ColumnName,
    override val parent: Entity
) : Column<T> {

    /**
     * Companion object with some important constants.
     */
    companion object {
        /** Record ID of the [ColumnV1Header]. */
        private const val HEADER_RECORD_ID: Long = 1L
    }

    /** The [Path] to the [Entity]'s main folder. */
    override val path: Path = parent.path.resolve("col_${name.simple}.db")

    /** Internal reference to the [Store] underpinning this [ColumnV1]. */
    private var store: CottontailStoreWAL = try {
        this.parent.parent.parent.config.mapdb.store(this.path)
    } catch (e: DBException) {
        throw DatabaseException("Failed to open column at '$path': ${e.message}'")
    }

    /** Internal reference to the [Header] of this [ColumnV1]. */
    private val header
        get() = this.store.get(HEADER_RECORD_ID, ColumnV1Header.Serializer)
            ?: throw DatabaseException.DataCorruptionException("Failed to open header of column '$name'!'")

    /**
     * Getter for this [ColumnV1]'s [ColumnDef]. Can be stored since [ColumnV1]s [ColumnDef] is immutable.
     *
     * @return [ColumnDef] for this [ColumnV1]
     */
    @Suppress("UNCHECKED_CAST")
    override val columnDef: ColumnDef<T> =
        this.header.let { ColumnDef(this.name, it.type as Type<T>, it.nullable) }

    /** The [DBOVersion] of this [ColumnV1]. */
    override val version: DBOVersion
        get() = DBOVersion.V1_0

    /** The [ColumnEngine] of this [ColumnV1]. */
    override val engine: ColumnEngine
        get() = ColumnEngine.MAPDB

    /**
     * The maximum tuple ID used by this [Column].
     */
    override val maxTupleId: Long
        get() = this.store.maxRecid

    /**
     * Status indicating whether this [ColumnV1] is open or closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    /** An internal lock that is used to synchronize structural changes to an [ColumnV1] (e.g. closing or deleting) with running [ColumnV1.Tx]. */
    private val closeLock = StampedLock()

    /**
     * Closes the [ColumnV1]. Closing an [ColumnV1] is a delicate matter since ongoing [ColumnV1.Tx]
     * are involved. Therefore, access to the method is mediated by an global [ColumnV1] wide lock.
     */
    override fun close() = this.closeLock.write {
        this.store.close()
        this.closed = true
    }

    /**
     * Creates a new [ColumnV1.Tx] and returns it.
     *
     * @param readonly True, if the resulting [ColumnV1.Tx] should be a read-only transaction.
     * @param tid The ID for the new [ColumnV1.Tx]
     *
     * @return A new [ColumnTransaction] object.
     */
    override fun newTx(context: TransactionContext): ColumnTx<T> = Tx(context)

    /**
     * A [Transaction] that affects this [ColumnV1].
     */
    inner class Tx constructor(override val context: TransactionContext) : ColumnTx<T> {

        /** Flag indicating whether or not this [Entity.Tx] was closed */
        @Volatile
        override var status: TxStatus = TxStatus.CLEAN
            private set

        /**
         * The [ColumnDef] of the [Column] underlying this [ColumnTransaction].
         *
         * @return [ColumnTransaction]
         */
        override val columnDef: ColumnDef<T>
            get() = this@ColumnV1.columnDef

        override val dbo: Column<T>
            get() = this@ColumnV1

        /** Tries to acquire a global read-lock on the [ColumnV1]. */
        init {
            if (this@ColumnV1.closed) {
                throw java.lang.IllegalStateException("")
            }
        }

        /** The [Serializer] used for de-/serialization of [ColumnV1] entries. */
        private val serializer = this@ColumnV1.type.serializerFactory().mapdb(this@ColumnV1.type.logicalSize)

        /** Obtains a global (non-exclusive) read-lock on [ColumnV1]. Prevents enclosing [ColumnV1] from being closed while this [ColumnV1.Tx] is still in use. */
        private val globalStamp = this@ColumnV1.closeLock.readLock()

        /** A [ReentrantReadWriteLock] local to this [Entity.Tx]. It makes sure, that this [Entity] cannot be committed, closed or rolled back while it is being used. */
        private val localLock = StampedLock()

        /**
         * Commits all changes made through this [Tx] since the last commit or rollback.
         */
        @Synchronized
        override fun commit() = this.localLock.write {
            if (this.status == TxStatus.DIRTY) {
                this@ColumnV1.store.commit()
                this.status = TxStatus.CLEAN
            }
        }

        /**
         * Rolls all changes made through this [Tx] back to the last commit. Can only be executed, if [Tx] is
         * in status [TxStatus.DIRTY] or [TxStatus.ERROR].
         */
        @Synchronized
        override fun rollback() = this.localLock.write {
            if (this.status == TxStatus.DIRTY || this.status == TxStatus.ERROR) {
                this@ColumnV1.store.rollback()
                this.status = TxStatus.CLEAN
            }
        }

        /**
         * Closes this [Tx] and relinquishes the associated [ReentrantReadWriteLock].
         */
        @Synchronized
        override fun close() = this.localLock.write {
            if (this.status != TxStatus.CLOSED) {
                if (this.status == TxStatus.DIRTY || this.status == TxStatus.ERROR) {
                    this.rollback()
                }
                this.status = TxStatus.CLOSED
                this@ColumnV1.closeLock.unlockRead(this.globalStamp)
            }
        }

        /**
         * Gets and returns an entry from this [ColumnV1].
         *
         * @param tupleId The ID of the desired entry
         * @return The desired entry.
         *
         * @throws DatabaseException If the tuple with the desired ID doesn't exist OR is invalid.
         */
        override fun read(tupleId: Long): T? = this.localLock.read {
            checkValidForRead()
            checkValidTupleId(tupleId)
            return this@ColumnV1.store.get(tupleId, this.serializer)
        }

        /**
         * Returns the number of entries in this [ColumnV1]. Action acquires a global read dataLock for the [ColumnV1].
         *
         * @return The number of entries in this [ColumnV1].
         */
        override fun count(): Long = this.localLock.read {
            checkValidForRead()
            return this@ColumnV1.header.count
        }

        /**
         * Creates and returns a new [Iterator] for this [ColumnV1.Tx] that returns
         * all [TupleId]s contained within the surrounding [ColumnV1].
         *
         * @return [Iterator]
         */
        override fun scan() = this.scan(1L..this@ColumnV1.maxTupleId)

        /**
         * Creates and returns a new [Iterator] for this [ColumnV1.Tx] that returns
         * all [TupleId]s contained within the surrounding [ColumnV1] and a certain range.
         *
         * @param range The [LongRange] that should be scanned.
         * @return [Iterator]
         */
        override fun scan(range: LongRange) = object : Iterator<TupleId> {

            init {
                checkValidForRead()
            }

            /** Wraps a [RecordIdIterator] from the [ColumnV1]. */
            private val wrapped = this@ColumnV1.store.RecordIdIterator(range)

            /**
             * Returns the next element in the iteration.
             */
            override fun next(): TupleId {
                return this.wrapped.next()
            }

            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean {
                return this.wrapped.hasNext()
            }
        }

        override fun insert(record: T?): Long = this.localLock.read {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun update(tupleId: Long, value: T?) = this.localLock.read {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun compareAndUpdate(tupleId: Long, value: T?, expected: T?): Boolean =
            this.localLock.read {
                throw UnsupportedOperationException("Operation not supported on legacy DBO.")
            }

        override fun delete(tupleId: Long) = this.localLock.read {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        /**
         * Checks if the provided tupleID is valid. Otherwise, an exception will be thrown.
         */
        private fun checkValidTupleId(tupleId: Long) {
            if ((tupleId < 0L) or (tupleId == HEADER_RECORD_ID)) {
                throw IllegalStateException("")
            }
        }

        /**
         * Checks if this [ColumnV1.Tx] is still open. Otherwise, an exception will be thrown.
         */
        @Synchronized
        private fun checkValidForRead() {
            if (this.status == TxStatus.CLOSED) throw IllegalStateException("")
            if (this.status == TxStatus.ERROR) throw IllegalStateException("")
        }
    }
}