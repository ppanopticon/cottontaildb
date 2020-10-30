package org.vitrivr.cottontail.database.column.hare

import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.column.ColumnTransaction
import org.vitrivr.cottontail.database.column.mapdb.MapDBColumn
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.Transaction
import org.vitrivr.cottontail.database.general.TransactionStatus
import org.vitrivr.cottontail.model.basics.CloseableIterator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnFile
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.StampedLock

/**
 * Represents a single column in the Cottontail DB model. A [HareColumn] record is identified by a tuple ID (long)
 * and can hold an arbitrary value. Usually, multiple [MapDBColumn]s make up an [Entity].
 *
 * @see Entity
 *
 * @param <T> Type of the value held by this [MapDBColumn].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class HareColumn<T : Value>(override val name: Name.ColumnName, override val parent: Entity) : Column<T> {

    /** The [Path] to the [HareColumn]'s main file. */
    override val path: Path = this.parent.path.resolve("col_${name.simple}.hare")

    /** The [FixedHareColumnFile] that backs this [HareColumn]. */
    private val column = FixedHareColumnFile<T>(this.path, true)

    /** This [HareColumn]'s [ColumnDef]. */
    override val columnDef: ColumnDef<T>
        get() = this.column.columnDef


    override val maxTupleId: Long
        get() = TODO("Not yet implemented")


    /** An internal lock that is used to synchronize concurrent read & write access to this [MapDBColumn] by different [MapDBColumn.Tx]. */
    private val txLock = StampedLock()

    /** An internal lock that is used to synchronize structural changes to an [MapDBColumn] (e.g. closing or deleting) with running [MapDBColumn.Tx]. */
    private val globalLock = StampedLock()

    /** Flag indicating whether this [HareColumn] is open or closed. */
    override var closed: Boolean = false
        private set

    override fun newTransaction(readonly: Boolean, tid: UUID): ColumnTransaction<T> {
        TODO("Not yet implemented")
    }

    /**
     * Closes the [HareColumn]. Closing an [HareColumn] is a delicate matter since ongoing [HareColumn.Tx] might be involved.
     * Therefore, access to the method is mediated by an global [HareColumn] wide lock.
     */
    override fun close() = this.globalLock.write {
        this.column.close()
        this.closed = true
    }

    /**
     * A [Transaction] that affects this [HareColumn].
     */
    inner class Tx constructor(override val readonly: Boolean, override val tid: UUID) : ColumnTransaction<T> {
        /** Flag indicating the [TransactionStatus] of this [HareColumn.Tx]. */
        @Volatile
        override var status: TransactionStatus = TransactionStatus.CLEAN
            private set

        /** The [ColumnDef] of the [Column] underlying this [ColumnTransaction]. */
        override val columnDef: ColumnDef<T>
            get() = this@HareColumn.columnDef

        /** Obtains a global (non-exclusive) read-lock on [HareColumn]. Prevents enclosing [HareColumn] from being closed while this [HareColumn.Tx] is still in use. */
        private val globalStamp = this@HareColumn.globalLock.readLock()

        /** Obtains transaction lock on [HareColumn]. Prevents concurrent read & write access to the enclosing [MapDBColumn]. */
        private val txStamp = if (this.readonly) {
            this@HareColumn.txLock.readLock()
        } else {
            this@HareColumn.txLock.writeLock()
        }

        /** A [StampedLock] local to this [HareColumn.Tx]. It makes sure, that this [HareColumn.Tx] cannot be committed, closed or rolled back while it is being used. */
        private val localLock = StampedLock()

        /**
         * Commits all changes made through this [Tx] since the last commit or rollback.
         */
        override fun commit() = this.localLock.write {
            if (this.status == TransactionStatus.DIRTY) {
                //this@HareColumn.column.commit()
                this.status = TransactionStatus.CLEAN
            }
        }

        /**
         * Rolls all changes made through this [Tx] back to the last commit. Can only be executed, if [Tx] is
         * in status [TransactionStatus.DIRTY] or [TransactionStatus.ERROR].
         */
        override fun rollback() = this.localLock.write {
            if (this.status == TransactionStatus.DIRTY || this.status == TransactionStatus.ERROR) {
                //this@HareColumn.column.rollback()
                this.status = TransactionStatus.CLEAN
            }
        }

        /**
         * Closes this [Tx] and relinquishes the associated [ReentrantReadWriteLock].
         */
        override fun close() = this.localLock.write {
            if (this.status != TransactionStatus.CLOSED) {
                if (this.status == TransactionStatus.DIRTY || this.status == TransactionStatus.ERROR) {
                    this.rollback()
                }
                this.status = TransactionStatus.CLOSED
                this@HareColumn.txLock.unlock(this.txStamp)
                this@HareColumn.globalLock.unlockRead(this.globalStamp)
            }
        }

        override fun count(): Long = this.localLock.read {
            TODO()
        }

        override fun read(tupleId: Long): T? = this.localLock.read {
            TODO()
        }

        override fun insert(record: T?)= this.localLock.read {
            TODO()
        }

        override fun update(tupleId: TupleId, value: T?) {

        }

        override fun compareAndUpdate(tupleId: Long, value: T?, expected: T?): Boolean {
            TODO("Not yet implemented")
        }

        override fun delete(tupleId: TupleId) {
            TODO("Not yet implemented")
        }

        override fun scan(): CloseableIterator<Long> {
            TODO("Not yet implemented")
        }

        override fun scan(range: LongRange): CloseableIterator<Long> {
            TODO("Not yet implemented")
        }
    }
}