package org.vitrivr.cottontail.server.grpc.services

import com.google.protobuf.Empty
import io.grpc.Status
import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.execution.operators.system.ListLocksOperator
import org.vitrivr.cottontail.execution.operators.system.ListTransactionsOperator
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.TXNGrpc
import org.vitrivr.cottontail.grpc.TXNGrpcKt
import java.util.*
import kotlin.time.ExperimentalTime

/**
 * Implementation of [TXNGrpc.TXNImplBase], the gRPC endpoint for managing [TransactionManager.Transaction]s in Cottontail DB
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@ExperimentalTime
class TXNService constructor(override val manager: TransactionManager) : TXNGrpcKt.TXNCoroutineImplBase(), gRPCTransactionService {

    /**
     * gRPC endpoint for beginning an new [TransactionManager.Transaction].
     */
    override suspend fun begin(request: Empty): CottontailGrpc.TransactionId {
        val txn = this.manager.Transaction(TransactionType.USER)
        return CottontailGrpc.TransactionId.newBuilder().setValue(txn.txId).build()
    }

    /**
     * gRPC for committing a [TransactionManager.Transaction].
     */
    override suspend fun commit(request: CottontailGrpc.TransactionId): Empty {
        val txn = this.manager[request.value] /* Reuse existing transaction. */
        if (txn === null || txn.type !== TransactionType.USER) {
            val message = "COMMIT failed because USER transaction ${request.value} could not be obtained."
            throw Status.FAILED_PRECONDITION.withDescription(message).asException()
        }
        val queryId = request.queryId.ifEmpty { UUID.randomUUID().toString() }
        try {
            txn.commit()
            return Empty.getDefaultInstance()
        } catch (e: Throwable) {
            throw Status.INTERNAL.withDescription(formatMessage(txn, queryId, "Failed to execute COMMIT due to unexpected error: ${e.message}")).asException()
        }
    }

    /**
     * gRPC for rolling back a [TransactionManager.Transaction].
     */
    override suspend fun rollback(request: CottontailGrpc.TransactionId): Empty {
        val txn = this.manager[request.value]
        if (txn === null || txn.type !== TransactionType.USER) {
            val message = "ROLLBACK failed because USER transaction ${request.value} could not be obtained."
            throw Status.FAILED_PRECONDITION.withDescription(message).asException()
        }
        val queryId = request.queryId.ifEmpty { UUID.randomUUID().toString() }
        try {
            txn.rollback()
            return Empty.getDefaultInstance()
        } catch (e: Throwable) {
            throw Status.INTERNAL.withDescription(formatMessage(txn, queryId, "Failed to execute COMMIT due to unexpected error: ${e.message}")).asException()
        }
    }

    /**
     * gRPC for listing all [TransactionManager.Transaction]s.
     */
    override fun listTransactions(request: Empty): Flow<CottontailGrpc.QueryResponseMessage> = this.withTransactionContext(description = "LIST TRANSACTIONS") { tx, q ->
        executeAndMaterialize(tx, ListTransactionsOperator(this.manager), q, 0)
    }

    /**
     * gRPC for listing all active locks.
     */
    override fun listLocks(request: Empty): Flow<CottontailGrpc.QueryResponseMessage> = this.withTransactionContext(description = "LIST LOCKS") { tx, q ->
        executeAndMaterialize(tx, ListLocksOperator(this.manager.lockManager), q, 0)
    }
}