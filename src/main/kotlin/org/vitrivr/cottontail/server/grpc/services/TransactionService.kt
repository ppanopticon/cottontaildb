package org.vitrivr.cottontail.server.grpc.services

import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionStatus
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.exceptions.TransactionException
import java.util.*

/**
 * A facility common to all service that handle [TransactionManager.Transaction]s over gRPC.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
interface TransactionService {


    /** The [TransactionManager] reference for this [TransactionService]. */
    val manager: TransactionManager

    /**
     * Executes a provided action in the context of a [TransactionManager.Transaction], which is either resumed or created.
     *
     * @param txId The [CottontailGrpc.TransactionId] optionally, usually unwrapped from an incoming message.
     * @param action The action that should be executed.
     */
    fun withTransactionContext(txId: CottontailGrpc.TransactionId = CottontailGrpc.TransactionId.getDefaultInstance(), action: (tx: TransactionManager.Transaction, queryId: String) -> Unit) {
        /* Obtain transaction. */
        val context = if (txId === CottontailGrpc.TransactionId.getDefaultInstance()) {
            this.manager.Transaction(TransactionType.USER_IMPLICIT) /* Start new transaction. */
        } else {
            val txn = this.manager[txId.value] /* Reuse existing transaction. */
            if (txn === null || txn.type !== TransactionType.USER) {
                throw TransactionException.TransactionNotFoundException(txId.value)
            }
            txn
        }
        val queryId = if (txId === CottontailGrpc.TransactionId.getDefaultInstance() && !txId.queryId.isEmpty()) {
            txId.queryId
        } else {
            UUID.randomUUID().toString()
        }

        /* Execute action with context. */
        action(context, queryId)

        /* Finalize implicit user transactions. */
        if (context.type === TransactionType.USER_IMPLICIT) {
            if (context.state === TransactionStatus.ERROR) {
                context.rollback()
            } else {
                context.commit()
            }
        }
    }

    /**
     * Formats a default output message.
     *
     * @param tx The [TransactionManager.Transaction] the message was produced in.
     * @param queryId The query ID affected by the message.
     * @param message The actual message.
     */
    fun formatMessage(tx: TransactionManager.Transaction, queryId: String, message: String) = "[${tx.txId}, $queryId] $message"
}