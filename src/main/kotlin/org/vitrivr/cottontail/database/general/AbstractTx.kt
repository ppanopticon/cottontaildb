package org.vitrivr.cottontail.database.general

import org.vitrivr.cottontail.database.index.AbstractIndex
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.exceptions.TxException

/**
 * An abstract [Tx] implementation that provides some basic functionality.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
abstract class AbstractTx(override val context: TransactionContext) : Tx {
    /** Flag indicating whether or not this [IndexTx] was closed */
    @Volatile
    final override var status: TxStatus = TxStatus.CLEAN
        protected set

    /** The [TxSnapshot] that captures changes made through this [AbstractIndex] not visible to the surrounding [DBO]. */
    protected abstract val snapshot: TxSnapshot

    /**
     * Commits all changes made through this [AbstractTx] and releases all locks obtained.
     *
     * This implementation only makes structural changes to the [AbstractTx] (updates status,
     * sanity checks etc). Implementing classes need to implement [TxSnapshot] to execute the
     * actual commit.
     */
    final override fun commit() {
        if (this.status == TxStatus.DIRTY) {
            this.snapshot.commit()
            this.status = TxStatus.CLEAN
        }
    }

    /**
     * Makes a rollback on all made through this [AbstractTx] and releases all locks obtained.
     *
     * This implementation only makes structural changes to the [AbstractTx] (updates status,
     * sanity checks etc). Implementing classes need to implement [TxSnapshot] to execute the actual
     * commit.
     */
    final override fun rollback() {
        if (this.status == TxStatus.DIRTY || this.status == TxStatus.ERROR) {
            this.snapshot.rollback()
            this.status = TxStatus.CLEAN
        }
    }

    /**
     * Closes this [AbstractTx]. If there are uncommitted changes, these changes will be rolled back.
     * Closed [AbstractTx] cannot be used anymore!
     */
    final override fun close() {
        if (this.status != TxStatus.CLOSED) {
            this.rollback()
            this.cleanup()
            this.status = TxStatus.CLOSED
        }
    }

    /**
     * Cleans all local resources obtained by this [AbstractTx] implementation. Called as part of and
     * prior to finalizing the [close] operation
     *
     * Implementers of this method may safely assume that upon reaching this method, all necessary locks on
     * Cottontail DB's data structures have been obtained to safely perform the CLOSE operation.
     */
    protected abstract fun cleanup()

    /**
     * Checks if this [AbstractIndex.Tx] is in a valid state for write operations to happen and sets its
     * [status] to [TxStatus.DIRTY]
     */
    protected inline fun <T> withWriteLock(block: () -> (T)): T {
        if (this.status == TxStatus.CLOSED) throw TxException.TxClosedException(this.context.txId)
        if (this.status == TxStatus.ERROR) throw TxException.TxInErrorException(this.context.txId)
        if (this.context.lockOn(this.dbo) !== LockMode.EXCLUSIVE) {
            this.context.requestLock(this.dbo, LockMode.EXCLUSIVE)
        }
        if (this.status != TxStatus.DIRTY) {
            this.status = TxStatus.DIRTY
        }
        return block()
    }

    /**
     * Checks if this [AbstractIndex.Tx] is in a valid state for read operations to happen.
     */
    protected inline fun <T> withReadLock(block: () -> (T)): T {
        if (this.status == TxStatus.CLOSED) throw TxException.TxClosedException(this.context.txId)
        if (this.status == TxStatus.ERROR) throw TxException.TxInErrorException(this.context.txId)
        if (this.context.lockOn(this.dbo) === LockMode.NO_LOCK) {
            this.context.requestLock(this.dbo, LockMode.SHARED)
        }
        return block()
    }
}