package org.vitrivr.cottontail.database.general

import org.vitrivr.cottontail.execution.TransactionContext

/**
 * An object that acts as unit of isolation for accesses (read/write) to the underlying [DBO].
 * [Tx] objects are bound to a specific [TransactionContext] that manages shared resources
 * (e.g. multiple, concurrent [Tx] objects) and locks to the [DBO]s.
 *
 * [Tx] can be closed, committed and rolled back.
 *
 * This interface defines the basic operations supported by a [Tx]. However, it does not  dictate
 * the isolation level. It is up to the implementation to define and implement the desired
 * level of isolation and obtaining the necessary locks to safely executed on operation.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
interface Tx : AutoCloseable {
    /** [TransactionContext] this [Tx] takes place in. */
    val context: TransactionContext

    /** The [DBO] this [Tx] belongs to. */
    val dbo: DBO

    /** [TxStatus] of this [Tx]. */
    val status: TxStatus

    /** Commits the transaction, saving all changes. Causes the [Tx] to complete and close. */
    fun commit()

    /** Rolls the [Tx], causing changes to be lost. Causes the [Tx] to complete and close. */
    fun rollback()
}