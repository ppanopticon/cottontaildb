package org.vitrivr.cottontail.server.grpc.services

import com.google.protobuf.Empty
import io.grpc.Status

import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory

import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.execution.operators.system.ListLocksOperator
import org.vitrivr.cottontail.execution.operators.system.ListTransactionsOperator
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.TXNGrpc
import org.vitrivr.cottontail.model.exceptions.TransactionException
import org.vitrivr.cottontail.server.grpc.operators.SpoolerSinkOperator
import java.util.*

/**
 * Implementation of [TXNGrpc.TXNImplBase], the gRPC endpoint for managing [TransactionManager.Transaction]s
 * in Cottontail DB
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class TXNService(override val manager: TransactionManager) : TXNGrpc.TXNImplBase(), TransactionService {

    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(TXNService::class.java)
    }

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
    override fun commit(request: CottontailGrpc.TransactionId, responseObserver: StreamObserver<Empty>) = try {
        this.withTransactionContext(request) { tx, q ->
            tx.commit()
            responseObserver.onCompleted()
        }
    } catch (e: TransactionException.TransactionNotFoundException) {
        val message = "Execution failed because transaction ${request.value} could not be resumed."
        LOGGER.info(message)
        responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(message).asException())
    }

    /**
     * gRPC for rolling back a [TransactionManager.Transaction].
     */
    override fun rollback(request: CottontailGrpc.TransactionId, responseObserver: StreamObserver<Empty>) = try {
        this.withTransactionContext(request) { tx, q ->
            tx.rollback()
            responseObserver.onCompleted()
        }
    } catch (e: TransactionException.TransactionNotFoundException) {
        val message = "Execution failed because transaction ${request.value} could not be resumed."
        LOGGER.info(message)
        responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(message).asException())
    }

    /**
     * gRPC for listing all [TransactionManager.Transaction]s.
     */
    override fun listTransactions(request: Empty, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext function@{ tx, q ->
        val operator = SpoolerSinkOperator(ListTransactionsOperator(this.manager), q, 0, responseObserver)
        tx.execute(operator)
        responseObserver.onCompleted()
    }

    /**
     * gRPC for listing all [TransactionManager.Transaction]s.
     */
    override fun listLocks(request: Empty, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext function@{ tx, q ->
        val operator = SpoolerSinkOperator(ListLocksOperator(this.manager.lockManager), q, 0, responseObserver)
        tx.execute(operator)
        responseObserver.onCompleted()
    }
}