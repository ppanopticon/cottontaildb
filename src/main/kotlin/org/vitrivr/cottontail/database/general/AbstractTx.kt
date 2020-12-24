package org.vitrivr.cottontail.database.general

import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.exceptions.TxException
import org.vitrivr.cottontail.model.recordset.Recordset

/**
 * An abstract [Tx] implementation that provides some basic functionality.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
@Suppress("OVERRIDE_BY_INLINE")
abstract class AbstractTx(override val context: TransactionContext) : Tx {
    /** Flag indicating whether or not this [IndexTx] was closed */
    @Volatile
    final override var status: TxStatus = TxStatus.CLEAN
        protected set

    /**
     * Commits all changes made through this [AbstractTx] and releases all locks obtained.
     *
     * This implementation only makes structural changes to the [AbstractTx]. Implementing
     * classes need to implement [performCommit] to execute the actual commit.
     */
    final override fun commit() {
        if (this.status == TxStatus.DIRTY) {
            this.performCommit()
            this.status = TxStatus.CLEAN
        }
        this.context.releaseLock(this.dbo)
    }

    /**
     * Makes a rollback on all made through this [AbstractTx] and releases all locks obtained.
     *
     *  This implementation only makes structural changes to the [AbstractTx]. Implementing
     * classes need to implement [performRollback] to execute the actual commit.
     */
    final override fun rollback() {
        if (this.status == TxStatus.DIRTY || this.status == TxStatus.ERROR) {
            this.performRollback()
            this.status = TxStatus.CLEAN
        }
        this.context.releaseLock(this.dbo)
    }

    /**
     * Closes this [AbstractTx]. If there are uncommitted changes, these changes will be rolled back.
     * Closed [AbstractTx] cannot be used anymore!
     */
    final override fun close() {
        if (this.status != TxStatus.CLOSED) {
            this.rollback()
            this.status = TxStatus.CLOSED
            this.cleanup()
        }
    }

    /**
     * Performs the actual COMMIT operation.
     *
     * Implementers of this method may safely assume that upon reaching this method, all necessary
     * locks on Cottontail DB's data structures have been obtained to safely perform the COMMIT operation.
     * Furthermore, this operation will only be called if the [status] is equal to [TxStatus.DIRTY]
     */
    protected abstract fun performCommit()

    /**
     * Performs the actual ROLLBACK operation.
     *
     * Implementers of this method may safely assume that upon reaching this method, all necessary
     * locks on Cottontail DB's data structures have been obtained to safely perform the ROLLBACK operation.
     * Furthermore, this operation will only be called if the [status] is equal to [TxStatus.DIRTY] or [TxStatus.ERROR]
     */
    protected abstract fun performRollback()

    /**
     * Cleans all local resources obtained by this [AbstractTx] implementation. Called as part of and
     * prior to finalizing the [close] operation
     *
     * Implementers of this method may safely assume that upon reaching this method, all necessary locks on
     * Cottontail DB's data structures have been obtained to safely perform the CLOSE operation.
     */
    protected abstract fun cleanup()

    /**
     * Checks if this [Index.Tx] is in a valid state for write operations to happen and sets its
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
     * Checks if this [Index.Tx] is in a valid state for read operations to happen.
     */
    protected inline fun <T> withReadLock(block: () -> (T)): T {
        if (this.status == TxStatus.CLOSED) throw TxException.TxClosedException(this.context.txId)
        if (this.status == TxStatus.ERROR) throw TxException.TxInErrorException(this.context.txId)
        if (this.context.lockOn(this.dbo) === LockMode.NO_LOCK) {
            this.context.requestLock(this.dbo, LockMode.SHARED)
        }
        return block()
    }

    /**
     * An inline function that can be used to create a transactional context from a [Tx].
     *
     * The provided block will be executed as a [Tx] and any exception thrown in the block will result
     * in a rollback. Once the block has been executed successfully, the [Tx] is committed.
     *
     * In both cases, the [Tx] that has been used will be closed.
     *
     * @param block The block that should be executed in a [Tx] context.
     */
    final override inline fun begin(block: (tx: Tx) -> Boolean) = try {
        if (block(this)) {
            commit()
        } else {
            rollback()
        }
    } catch (e: Throwable) {
        rollback()
        throw e
    } finally {
        close()
    }

    /**
     * An inline function that can be used to create a transactional context from a [Tx].
     *
     * The provided block will be executed as a [Tx] and any exception thrown in the block will result
     * in a rollback. Once the block has been executed successfully, the [Tx] is committed and a [Recordset]
     * will be returned.
     *
     * In both cases, the [Tx] that has been used will be closed.
     *
     * @param block The block that should be executed in a [Tx] context.
     * @return The [Recordset] that resulted from the [Tx].
     */
    final override inline fun query(block: (tx: Tx) -> Recordset): Recordset? = try {
        val result = block(this)
        commit()
        result
    } catch (e: Throwable) {
        rollback()
        throw e
    } finally {
        close()
    }
}