package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionStatus
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.util.*

/**
 * A facility common to all service that handle [TransactionManager.Transaction]s over gRPC.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
interface TransactionService {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(TransactionService::class.java)
    }

    /** The [TransactionManager] reference for this [TransactionService]. */
    val manager: TransactionManager

    /**
     * Executes a provided gRPC action in the context of a [TransactionManager.Transaction], which is either resumed or created.
     *
     * @param txId The [CottontailGrpc.TransactionId] optionally, usually unwrapped from an incoming message.
     * @param responseObserver To communicate with.
     * @param action The action that should be executed.
     */
    fun withTransactionContext(
        txId: CottontailGrpc.TransactionId = CottontailGrpc.TransactionId.getDefaultInstance(),
        responseObserver: StreamObserver<*>,
        action: (tx: TransactionManager.Transaction, queryId: String) -> Status,
    ) {
        /* Obtain transaction + query Id. */
        val context = if (txId === CottontailGrpc.TransactionId.getDefaultInstance()) {
            this.manager.Transaction(TransactionType.USER_IMPLICIT) /* Start new transaction. */
        } else {
            val txn = this.manager[txId.value] /* Reuse existing transaction. */
            if (txn === null || txn.type !== TransactionType.USER) {
                val message = "Execution failed because transaction ${txId.value} could not be resumed."
                LOGGER.warn(message)
                responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(message).asException())
                return
            }
            txn
        }
        val queryId =
            if (txId === CottontailGrpc.TransactionId.getDefaultInstance() && txId.queryId.isNotEmpty()) {
                txId.queryId
            } else {
                UUID.randomUUID().toString()
            }

        /* Execute action with context; finalize implicit user transactions. */
        val status: Status = action(context, queryId)
        if (context.type === TransactionType.USER_IMPLICIT) {
            if (context.state == TransactionStatus.READY) {
                context.commit()
            } else if (context.state == TransactionStatus.ERROR) {
                context.rollback()
            }
        }

        /* Log status. */
        when (status.code) {
            Status.Code.UNKNOWN,
            Status.Code.DATA_LOSS -> LOGGER.error(status.description)
            Status.Code.INTERNAL -> LOGGER.warn(status.description)
            else -> if (status.description != null) LOGGER.info(status.description)
        }

        /* Finalize gRPC communication. */
        if (status.code != Status.Code.OK) {
            responseObserver.onError(status.asRuntimeException())
        } else {
            responseObserver.onCompleted()
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