package org.vitrivr.cottontail.model.exceptions

import org.vitrivr.cottontail.model.basics.TransactionId

/**
 * A type of [DatabaseException]. [TransactionException]s are thrown whenever there is a problem on
 * the level of [org.vitrivr.cottontail.execution.TransactionManager.Transaction]
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
open class TransactionException(val txId: TransactionId, message: String) : DatabaseException(message) {
    /**
     * Thrown whenever trying to access a [org.vitrivr.cottontail.execution.TransactionManager.Transaction] that could not be found.
     */
    class DeadlockException(txId: TransactionId, e: org.vitrivr.cottontail.database.locking.DeadlockException) : TransactionException(txId, e.message!!)

    /**
     * Thrown whenever trying to access a [org.vitrivr.cottontail.execution.TransactionManager.Transaction] that could not be found.
     */
    class TransactionNotFoundException(txId: TransactionId) : TransactionException(txId, "Transaction with $txId could not be found.")
}