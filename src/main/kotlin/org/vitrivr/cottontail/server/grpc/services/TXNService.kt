package org.vitrivr.cottontail.server.grpc.services

import com.google.protobuf.Empty

import io.grpc.Status
import io.grpc.stub.StreamObserver

import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.execution.operators.sinks.SpoolerSinkOperator
import org.vitrivr.cottontail.execution.operators.system.ListLocksOperator
import org.vitrivr.cottontail.execution.operators.system.ListTransactionsOperator
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.TXNGrpc

/**
 * Implementation of [TXNGrpc.TXNImplBase], the gRPC endpoint for managing [TransactionManager.Transaction]s in Cottontail DB
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class TXNService(override val manager: TransactionManager) : TXNGrpc.TXNImplBase(), TransactionService {


    /**
     * gRPC endpoint for beginning an new [TransactionManager.Transaction].
     */
    override fun begin(request: Empty, responseObserver: StreamObserver<CottontailGrpc.TransactionId>) {
        val txn = this.manager.Transaction(TransactionType.USER)
        val txId = CottontailGrpc.TransactionId.newBuilder().setValue(txn.txId).build()
        responseObserver.onNext(txId)
        responseObserver.onCompleted()
    }

    /**
     * gRPC for committing a [TransactionManager.Transaction].
     */
    override fun commit(request: CottontailGrpc.TransactionId, responseObserver: StreamObserver<Empty>) = this.withTransactionContext(request, responseObserver) { tx, q ->
        try {
            tx.commit()
            responseObserver.onNext(Empty.getDefaultInstance())
            Status.OK
        } catch (e: Throwable) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Failed to execute COMMIT due to unexpected error: ${e.message}"))
        }
    }

    /**
     * gRPC for rolling back a [TransactionManager.Transaction].
     */
    override fun rollback(request: CottontailGrpc.TransactionId, responseObserver: StreamObserver<Empty>) = this.withTransactionContext(request, responseObserver) { tx, q ->
        try {
            tx.rollback()
            responseObserver.onNext(Empty.getDefaultInstance())
            Status.OK
        } catch (e: Throwable) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Failed to execute ROLLBACK due to unexpected error: ${e.message}"))
        }
    }

    /**
     * gRPC for listing all [TransactionManager.Transaction]s.
     */
    override fun listTransactions(request: Empty, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(responseObserver = responseObserver) { tx, q ->
        try {
            val operator = SpoolerSinkOperator(ListTransactionsOperator(this.manager), q, 0, responseObserver)
            tx.execute(operator)
            Status.OK
        } catch (e: Throwable) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Failed to list locks due to unexpected error: ${e.message}"))
        }
    }

    /**
     * gRPC for listing all [TransactionManager.Transaction]s.
     */
    override fun listLocks(request: Empty, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(responseObserver = responseObserver) { tx, q ->
        try {
            val operator = SpoolerSinkOperator(ListLocksOperator(this.manager.lockManager), q, 0, responseObserver)
            tx.execute(operator)
            Status.OK
        } catch (e: Throwable) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Failed to list locks due to unexpected error: ${e.message}"))
        }
    }
}