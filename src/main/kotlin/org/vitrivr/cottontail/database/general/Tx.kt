package org.vitrivr.cottontail.database.general

import org.vitrivr.cottontail.execution.TransactionContext

/**
 * An object that acts as unit of isolation for accesses (read/write) to the underlying [DBO].
 * [Tx] can be closed, committed and rolled back.
 *
 * [Tx] objects are bound to a specific [TransactionContext] that manages shared resources
 * (e.g. multiple, concurrent [Tx] objects) and locks to the [DBO]s.
 *
 * This interface defines the basic operations supported by a [Tx]. However, it does not dictate
 * the isolation level. It is up to the implementation to define and implement the desired
 * level of isolation and to obtain the necessary locks to safely execute an operation.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
interface Tx : AutoCloseable {
    /** The [DBO] this [Tx] belongs to. */
    val dbo: DBO

    /** [TransactionContext] this [Tx] takes place in. */
    val context: TransactionContext

    /** The [TxSnapshot] that captures changes made through this [Tx] that may not yet be visible to the surrounding [DBO]. */
    val snapshot: TxSnapshot

    /** [TxStatus] of this [Tx]. */
    val status: TxStatus

    /** Commits the transaction, saving all changes. Causes the [Tx] to complete and close. */
    fun commit()

    /** Rolls the [Tx], causing changes to be lost. Causes the [Tx] to complete and close. */
    fun rollback()
}