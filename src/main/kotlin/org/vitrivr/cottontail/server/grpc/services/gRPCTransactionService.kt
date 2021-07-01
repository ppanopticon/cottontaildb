package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import kotlinx.coroutines.flow.*
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.client.language.basics.Constants
import org.vitrivr.cottontail.database.locking.DeadlockException
import org.vitrivr.cottontail.database.queries.binding.extensions.toLiteral
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionStatus
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.ExecutionException
import org.vitrivr.cottontail.model.exceptions.QueryException
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.TimeSource

/**
 * A facility common to all service that handle [TransactionManager.Transaction]s over gRPC.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
@ExperimentalTime
interface gRPCTransactionService {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(gRPCTransactionService::class.java)
    }

    /** The [TransactionManager] reference for this [gRPCTransactionService]. */
    val manager: TransactionManager

    /**
     * Executes a provided gRPC action in the context of a [TransactionManager.Transaction], which is either resumed or created.
     *
     * @param txId The [CottontailGrpc.TransactionId] optionally, usually unwrapped from an incoming message.
     * @param description String describing the operation that is being execugted
     * @param action The action that should be executed.
     */
    fun withTransactionContext(
        txId: CottontailGrpc.TransactionId = CottontailGrpc.TransactionId.getDefaultInstance(),
        description: String,
        action: (tx: TransactionManager.Transaction, queryId: String) -> Flow<CottontailGrpc.QueryResponseMessage>
    ): Flow<CottontailGrpc.QueryResponseMessage> {
        /* Obtain transaction + query Id. */
        val context = if (txId === CottontailGrpc.TransactionId.getDefaultInstance()) {
            this.manager.Transaction(TransactionType.USER_IMPLICIT) /* Start new transaction. */
        } else {
            val txn = this.manager[txId.value] /* Reuse existing transaction. */
            if (txn === null || txn.type !== TransactionType.USER) {
                val message = "Execution failed because transaction ${txId.value} could not be resumed."
                LOGGER.warn(message)
                return flow { throw Status.FAILED_PRECONDITION.withDescription(message).asException() }
            }
            txn
        }
        val queryId = if (txId === CottontailGrpc.TransactionId.getDefaultInstance() && txId.queryId.isNotEmpty()) {
            txId.queryId
        } else {
            UUID.randomUUID().toString()
        }

        /* Prepare flow for execution. If this fails, implicit transactions are automatically rolled back. */
        val mark = TimeSource.Monotonic.markNow()
        val flow = try {
            action(context, queryId)
        } catch (e: Throwable) {
            if (context.type === TransactionType.USER_IMPLICIT) {
                context.rollback()
            }
            throw when (e) {
                is QueryException.QuerySyntaxException -> Status.INVALID_ARGUMENT.withDescription(formatMessage(context, queryId, "$description failed because of syntax error. ${e.message}")).withCause(e).asException()
                is QueryException.QueryBindException -> Status.INVALID_ARGUMENT.withDescription(formatMessage(context, queryId, "$description failed because of binding error. ${e.message}")).withCause(e).asException()
                is QueryException.QueryPlannerException -> Status.INVALID_ARGUMENT.withDescription(formatMessage(context, queryId, "$description failed because of syntax error. ${e.message}")).withCause(e).asException()
                is DatabaseException -> Status.INTERNAL.withDescription(formatMessage(context, queryId, "$description failed because of a database error. ${e.message}")).withCause(e).asException()
                else -> {
                    e.printStackTrace()
                    Status.UNKNOWN.withDescription(formatMessage(context, queryId, "$description failed because of an unhandled exception.")).withCause(e).asException()
                }
            }
        }

        /* Return flow and execute it. */
        return flow.catch { e ->
            throw when (e) {
                is DeadlockException -> Status.ABORTED.withDescription(formatMessage(context, queryId, "$description failed because of a deadlock with another transaction. ${e.message}")).asException()
                is DatabaseException -> Status.INTERNAL.withDescription(formatMessage(context, queryId, "$description failed because of a database error. ${e.message}")).withCause(e).asException()
                is ExecutionException -> Status.INTERNAL.withDescription(formatMessage(context, queryId, "$description failed because of an execution error. ${e.message}")).withCause(e).asException()
                is StatusRuntimeException,
                is StatusException -> e
                else -> {
                    e.printStackTrace()
                    Status.UNKNOWN.withDescription(formatMessage(context, queryId, "$description failed because of an unhandled exception.")).withCause(e).asException()
                }
            }
        }.onCompletion {
            if (context.type === TransactionType.USER_IMPLICIT) {
                if (context.state == TransactionStatus.READY) {
                    context.commit()
                } else if (context.state == TransactionStatus.ERROR) {
                    context.rollback()
                }
            }
            if (it == null) {
                LOGGER.info(formatMessage(context, queryId, "$description completed successfully in ${mark.elapsedNow()}!"))
            } else {
                LOGGER.error(formatMessage(context, queryId, "$description failed: ${it.cause?.message}"))
            }
        }
    }

    /**
     * Executes the given [Operator] and materializes the results as [CottontailGrpc.QueryResponseMessage].
     *
     * @param operator The [Operator] to executed.
     * @param queryId The [String] ID that identifies the query.
     * @param queryIndex The query index.
     * @return [Flow] of [CottontailGrpc.QueryResponseMessage]
     */
    fun executeAndMaterialize(tx: TransactionManager.Transaction, operator: Operator, queryId: String, queryIndex: Int = 0): Flow<CottontailGrpc.QueryResponseMessage> {
        val columns = operator.columns.map {
            val name = CottontailGrpc.ColumnName.newBuilder().setName(it.name.simple)
            val entityName = it.name.entity()
            if (entityName != null) {
                name.setEntity(CottontailGrpc.EntityName.newBuilder().setName(entityName.simple).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(entityName.schema().simple)))
            }
            name.build()
        }

        return flow {
            val responseBuilder = CottontailGrpc.QueryResponseMessage.newBuilder().setTid(CottontailGrpc.TransactionId.newBuilder().setQueryId(queryId).setValue(tx.txId)).addAllColumns(columns)
            var accumulatedSize = 0L
            var results = 0
            tx.execute(operator).collect {
                val tuple = it.toTuple()
                results += 1
                if (accumulatedSize + tuple.serializedSize >= Constants.MAX_PAGE_SIZE_BYTES) {
                    emit(responseBuilder.build())
                    responseBuilder.clearTuples()
                    accumulatedSize = 0L
                }

                /* Add entry to page and increment counter. */
                responseBuilder.addTuples(tuple)
                accumulatedSize += tuple.serializedSize
            }

            /* Flush remaining tuples. */
            if (results == 0 || responseBuilder.tuplesCount > 0) {
                emit(responseBuilder.build())
            }
        }
    }

    /**
     * Converts a [Record] to a [CottontailGrpc.QueryResponseMessage.Tuple]
     */
    fun Record.toTuple(): CottontailGrpc.QueryResponseMessage.Tuple {
        val tuple = CottontailGrpc.QueryResponseMessage.Tuple.newBuilder()
        this.forEach { _, v -> tuple.addData(v?.toLiteral() ?: CottontailGrpc.Literal.newBuilder().setNullData(CottontailGrpc.Null.getDefaultInstance()).build()) }
        return tuple.build()
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