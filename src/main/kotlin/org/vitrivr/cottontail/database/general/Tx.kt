package org.vitrivr.cottontail.database.general

import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.recordset.Recordset

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
    fun begin(block: (tx: Tx) -> Boolean)

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
    fun query(block: (tx: Tx) -> Recordset): Recordset?
}