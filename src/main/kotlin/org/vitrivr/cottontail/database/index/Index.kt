package org.vitrivr.cottontail.database.index

import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.Transaction
import org.vitrivr.cottontail.database.general.TransactionStatus
import org.vitrivr.cottontail.database.queries.components.Predicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.TransactionException
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.util.*
import java.util.concurrent.locks.StampedLock

/**
 * Represents an [Index] structure in the Cottontail DB data model. An [Index] belongs to an [Entity]
 * and can be used to index one to many [Column]s. Usually, [Index]es allow for faster data access.
 *
 * @see Schema
 * @see Column
 * @see Entity.Tx
 *
 * @author Ralph Gasser
 * @version 1.6
 */
abstract class Index : DBO {

    /** An internal lock that is used to synchronize structural changes to an [Index] (e.g. closing or deleting) with running [Index.Tx]. */
    protected val globalLock = StampedLock()

    /** An internal lock that is used to synchronize concurrent read & write access to this [Index] by different [Index.Tx]. */
    protected val txLock = StampedLock()

    /** The [Name.IndexName] of this [Index]. */
    abstract override val name: Name.IndexName

    /** Reference to the [Entity], this [Index] belongs to. */
    abstract override val parent: Entity

    /** The [ColumnDef] that are covered (i.e. indexed) by this [Index]. */
    abstract val columns: Array<ColumnDef<*>>

    /**
     * The [ColumnDef] that are produced by this [Index]. They may differ from the indexed columns,
     * since some [Index] implementations only return a tuple ID OR because some implementations may
     * add some kind of score.
     */
    abstract val produces: Array<ColumnDef<*>>

    /** The type of [Index]. */
    abstract val type: IndexType

    /** True, if the [Index] supports incremental updates, and false otherwise. */
    abstract val supportsIncrementalUpdate: Boolean

    /**
     * Checks if this [Index] can process the provided [Predicate] and returns true if so and false otherwise.
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    abstract fun canProcess(predicate: Predicate): Boolean

    /**
     * Calculates the cost estimate if this [Index] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    abstract fun cost(predicate: Predicate): Cost

    /**
     * Handles finalization, in case the Garbage Collector reaps a cached [Index].
     */
    @Synchronized
    protected fun finalize() {
        this.close()
    }

    /**
     * Opens and returns a new [IndexTransaction] object that can be used to interact with this [Index].
     *
     * @param parent If the [Entity.Tx] that requested the [IndexTransaction].
     */
    abstract fun begin(parent: Entity.Tx): IndexTransaction

    /**
     * A [Transaction] that affects this [Index].
     */
    protected abstract inner class Tx constructor(val parent: Entity.Tx) : IndexTransaction {

        /** Flag indicating whether or not this [IndexTransaction] was closed */
        @Volatile
        final override var status: TransactionStatus = TransactionStatus.CLEAN
            protected set

        /** Flag indicating whether this [IndexTransaction] is readonly. */
        final override val readonly: Boolean = parent.readonly

        init {
            if (this@Index.closed) {
                throw TransactionException.TransactionDBOClosedException(this.tid)
            }
        }

        /** The transaction ID of this [Index.Tx] is inherited by the parent [Entity.Tx]. */
        final override val tid: UUID
            get() = this.parent.tid

        /** Obtains a global (non-exclusive) read-lock on [Index]. Prevents enclosing [Index] from being closed while this [Index.Tx] is still in use. */
        protected val globalStamp = this@Index.globalLock.readLock()

        /** Obtains tx lock on [Index]. Prevents concurrent read & write access to the enclosing [Index]. */
        protected val txStamp = if (this.readonly) {
            this@Index.txLock.readLock()
        } else {
            this@Index.txLock.writeLock()
        }

        /** A local [StampedLock] that mediates access to this [Tx] and it being closed. */
        protected val localLock = StampedLock()

        /** The simple [Name]s of the [Index] that underpins this [IndexTransaction] */
        override val name: Name
            get() = this@Index.name

        /** The [ColumnDef]s covered by the [Index] that underpins this [IndexTransaction]. */
        override val columns: Array<ColumnDef<*>>
            get() = this@Index.columns

        /** The [ColumnDef]s returned by the [Index] that underpins this [IndexTransaction]. */
        override val produces: Array<ColumnDef<*>>
            get() = this@Index.produces

        /** The [IndexType] of the [Index] that underpins this [IndexTransaction]. */
        override val type: IndexType
            get() = this@Index.type

        /** Returns true, if the [Index] underpinning this [IndexTransaction] supports incremental updates, and false otherwise. */
        override val supportsIncrementalUpdate: Boolean
            get() = this@Index.supportsIncrementalUpdate

        /**
         * Checks if this [IndexTransaction] can process the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return True if [Predicate] can be processed, false otherwise.
         */
        override fun canProcess(predicate: Predicate): Boolean = this@Index.canProcess(predicate)

        /**
         * Commits all changes to the [Index] made through this [Index.Tx]
         */
        final override fun commit() = this.localLock.read {
            if (this.status == TransactionStatus.DIRTY) {
                this.performCommit()
                this.status = TransactionStatus.CLEAN
            }
        }

        /**
         * Performs the actual COMMIT operation. It is up to the implementing class to obtain locks on necessary
         * data structures and cleanup the transaction.
         *
         * Implementers of this method may safely assume that upon reaching this method, all necessary locks on
         * Cottontail DB's data structures have been obtained to safely perform the COMMIT operation on the [Index].
         * Furthermore, this operation will only be called if the [status] is equal to [TransactionStatus.DIRTY]
         */
        protected abstract fun performCommit()

        /**
         * Makes a rollback on all changes to the [Index] made through this [Index.Tx]
         */
        final override fun rollback() = this.localLock.read {
            if (this.status == TransactionStatus.DIRTY || this.status == TransactionStatus.ERROR) {
                this.performRollback()
                this.status = TransactionStatus.CLEAN
            }
        }

        /**
         * Performs the actual ROLLBACK operation. It is up to the implementing class to obtain locks on necessary
         * data structures and cleanup the transaction.
         *
         * Implementers of this method may safely assume that upon reaching this method, all necessary locks on
         * Cottontail DB's data structures have been obtained to safely perform the ROLLBACK operation on the [Index].
         * Furthermore, this operation will only be called if the [status] is equal to [TransactionStatus.DIRTY] or
         * [TransactionStatus.ERROR]
         */
        protected abstract fun performRollback()

        /**
         * Closes this [Index.Tx] and releases the global lock. If there are uncommitted changes, these changes
         * will be rolled back. Closed [Index.Tx] cannot be used anymore!
         */
        final override fun close() = this.localLock.write {
            if (this.status != TransactionStatus.CLOSED) {
                if (this.status == TransactionStatus.DIRTY || this.status == TransactionStatus.ERROR) {
                    this.performRollback()
                }
                this.status = TransactionStatus.CLOSED
                this@Index.txLock.unlock(this.txStamp)
                this@Index.globalLock.unlockRead(this.globalStamp)
            }
        }

        /**
         * Cleans all local resources obtained by this [Index] implementation. Called as part of and prior to finalizing
         * the [close] operation
         *
         * Implementers of this method may safely assume that upon reaching this method, all necessary locks on
         * Cottontail DB's data structures have been obtained to safely perform the CLOSE operation on the [Index].
         */
        protected abstract fun cleanup()

        /**
         * Checks if this [Index.Tx] is in a valid state for write operations to happen and sets its
         * [status] to [TransactionStatus.DIRTY]
         */
        protected fun checkValidForWrite() {
            if (this.readonly) throw TransactionException.TransactionReadOnlyException(tid)
            if (this.status == TransactionStatus.CLOSED) throw TransactionException.TransactionClosedException(tid)
            if (this.status == TransactionStatus.ERROR) throw TransactionException.TransactionInErrorException(tid)
            if (this.status != TransactionStatus.DIRTY) {
                this.status = TransactionStatus.DIRTY
            }
        }

        /**
         * Checks if this [Index.Tx] is in a valid state for read operations to happen.
         */
        protected fun checkValidForRead() {
            if (this.status == TransactionStatus.CLOSED) throw TransactionException.TransactionClosedException(tid)
            if (this.status == TransactionStatus.ERROR) throw TransactionException.TransactionInErrorException(tid)
        }
    }
}