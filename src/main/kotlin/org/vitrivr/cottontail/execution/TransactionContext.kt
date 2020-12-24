package org.vitrivr.cottontail.execution

import kotlinx.coroutines.CoroutineDispatcher
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.Tx
import org.vitrivr.cottontail.database.locking.Lock
import org.vitrivr.cottontail.database.locking.LockManager
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.execution.TransactionManager.Transaction
import org.vitrivr.cottontail.model.basics.TransactionId

/**
 * A [TransactionContext] used by operators and their [Txn]s to execute and obtain necessary locks
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface TransactionContext {

    /** The [TransactionId] for this [TransactionContext]. */
    val txId: TransactionId

    /** Reference to the [TransactionManager]s [CoroutineDispatcher].*/
    val dispatcher: CoroutineDispatcher

    /**
     * Obtains a [Tx] for the given [DBO]. This method should make sure, that only one [Tx] per [DBO] is created.
     *
     * @param dbo The DBO] to create the [Tx] for.
     * @return The resulting [Tx]
     */
    fun getTx(dbo: DBO): Tx

    /**
     * Acquires a [Lock] on a [DBO] for the given [LockMode]. This call is delegated to the
     * [LockManager] and really just a convenient way for [Tx] objects to obtain locks.
     *
     * @param dbo [DBO] The [DBO] to request the lock for.
     * @param mode The desired [LockMode]
     */
    fun requestLock(dbo: DBO, mode: LockMode)

    /**
     * Releases a [Lock] on a [DBO]. This call is delegated to the [LockManager] and really just
     * a convenient way for [Tx] objects to obtain locks.
     *
     * @param dbo [DBO] The [DBO] to release the lock for.
     */
    fun releaseLock(dbo: DBO)

    /**
     * Returns the [LockMode] this [Transaction] has on the given [DBO].
     *
     * @param dbo [DBO] The [DBO] to query the [LockMode] for.
     * @return [LockMode]
     */
    fun lockOn(dbo: DBO): LockMode
}