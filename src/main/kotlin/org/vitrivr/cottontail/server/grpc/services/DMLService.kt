package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.CottontailQueryPlanner
import org.vitrivr.cottontail.database.queries.planning.rules.logical.LeftConjunctionRewriteRule
import org.vitrivr.cottontail.database.queries.planning.rules.logical.RightConjunctionRewriteRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation.DeleteImplementationRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation.EntityScanImplementationRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation.FilterImplementationRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation.UpdateImplementationRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.index.BooleanIndexScanRule
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DMLGrpc
import org.vitrivr.cottontail.model.exceptions.ExecutionException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.exceptions.TransactionException
import org.vitrivr.cottontail.server.grpc.helper.GrpcQueryBinder
import org.vitrivr.cottontail.server.grpc.operators.SpoolerSinkOperator
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime
import kotlin.time.measureTimedValue

/**
 * Implementation of [DMLGrpc.DMLImplBase], the gRPC endpoint for inserting data into Cottontail DB [Entity]s.
 *
 * @author Ralph Gasser
 * @version 1.3.1
 */
@ExperimentalTime
class DMLService(val catalogue: Catalogue, override val manager: TransactionManager) : DMLGrpc.DMLImplBase(), TransactionService {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(DMLService::class.java)
    }

    /** [GrpcQueryBinder] used to generate [org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalNodeExpression] tree from a gRPC query. */
    private val binder = GrpcQueryBinder(this.catalogue)

    /** [CottontailQueryPlanner] used to generate execution plans from query definitions. */
    private val planner = CottontailQueryPlanner(
            logicalRewriteRules = listOf(LeftConjunctionRewriteRule, RightConjunctionRewriteRule),
            physicalRewriteRules = listOf(BooleanIndexScanRule, EntityScanImplementationRule, FilterImplementationRule, DeleteImplementationRule, UpdateImplementationRule)
    )

    /**
     * gRPC endpoint for handling UPDATE queries.
     */
    override fun update(request: CottontailGrpc.UpdateMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = try {
        this.withTransactionContext(request.txId) { tx, q ->
            try {
                val totalDuration = measureTime {
                    /* Bind query and create logical plan. */
                    val bindTimedValue = measureTimedValue {
                        this.binder.parseAndBindUpdate(request, tx)
                    }
                    LOGGER.trace(formatMessage(tx, q, "Parsing & binding UPDATE took ${bindTimedValue.duration}."))

                    /* Plan query and create execution plan. */
                    val plannedTimedValue = measureTimedValue {
                        val candidates = this.planner.plan(bindTimedValue.value)
                        SpoolerSinkOperator(candidates.minByOrNull { it.totalCost }!!.toOperator(this.manager), q, 0, responseObserver)
                    }
                    LOGGER.trace(formatMessage(tx, q, "Planning UPDATE took ${plannedTimedValue.duration}."))

                    /* Execute UPDATE. */
                    tx.execute(plannedTimedValue.value)
                }

                /* Finalize invocation. */
                responseObserver.onCompleted()
                LOGGER.trace(formatMessage(tx, q, "Executing UPDATE took $totalDuration."))
            } catch (e: QueryException.QuerySyntaxException) {
                val message = formatMessage(tx, q, "UPDATE failed due to syntax error: ${e.message}")
                LOGGER.info(message)
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException())
            } catch (e: QueryException.QueryBindException) {
                val message = formatMessage(tx, q, "UPDATE failed due to binding error: ${e.message}")
                LOGGER.info(message)
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException())
            } catch (e: QueryException.QueryPlannerException) {
                val message = formatMessage(tx, q, "UPDATE failed because of an error during query planning: ${e.message}")
                LOGGER.info(message)
                responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            } catch (e: TransactionException.DeadlockException) {
                val message = formatMessage(tx, q, "UPDATE failed due to deadlock with other transaction: ${e.message}")
                LOGGER.info(message)
                responseObserver.onError(Status.ABORTED.withDescription(message).asException())
            } catch (e: ExecutionException) {
                val message = formatMessage(tx, q, "UPDATE failed due to execution error: ${e.message}")
                LOGGER.info(message)
                responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            } catch (e: Throwable) {
                val message = formatMessage(tx, q, "UPDATE failed due an unhandled error: ${e.message}")
                LOGGER.error(message, e)
                responseObserver.onError(Status.UNKNOWN.withDescription(message).asException())
            }
        }
    } catch (e: TransactionException.TransactionNotFoundException) {
        val message = "Execution failed because transaction ${request.txId.value} could not be resumed."
        LOGGER.info(message)
        responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(message).asException())
    }

    /**
     * gRPC endpoint for handling DELETE queries.
     */
    override fun delete(request: CottontailGrpc.DeleteMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = try {
        this.withTransactionContext(request.txId) { tx, q ->
            try {
                val totalDuration = measureTime {
                    /* Bind query and create logical plan. */
                    val bindTimedValue = measureTimedValue {
                        this.binder.parseAndBindDelete(request, tx)
                    }
                    LOGGER.trace(formatMessage(tx, q, "Parsing & binding DELETE took ${bindTimedValue.duration}."))

                    /* Plan query and create execution plan. */
                    val planTimedValue = measureTimedValue {
                        val candidates = this.planner.plan(bindTimedValue.value)
                        val operator = candidates.minByOrNull { it.totalCost }!!.toOperator(this.manager)
                        SpoolerSinkOperator(operator, q, 0, responseObserver)
                    }
                    LOGGER.trace(formatMessage(tx, q, "Planning DELETE took ${planTimedValue.duration}."))

                    /* Execute UPDATE. */
                    tx.execute(planTimedValue.value)
                }

                /* Finalize invocation. */
                responseObserver.onCompleted()
                LOGGER.trace(formatMessage(tx, q, "Executed DELETE in $totalDuration."))
            } catch (e: QueryException.QuerySyntaxException) {
                val message = formatMessage(tx, q, "DELETE failed due to syntax error: ${e.message}")
                LOGGER.info(message)
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException())
            } catch (e: QueryException.QueryBindException) {
                val message = formatMessage(tx, q, "DELETE failed due to binding error: ${e.message}")
                LOGGER.info(message)
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException())
            } catch (e: QueryException.QueryPlannerException) {
                val message = formatMessage(tx, q, "DELETE failed because of an error during query planning: ${e.message}")
                LOGGER.info(message)
                responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            } catch (e: TransactionException.DeadlockException) {
                val message = formatMessage(tx, q, "DELETE failed due to deadlock with other transaction: ${e.message}")
                LOGGER.info(message)
                responseObserver.onError(Status.ABORTED.withDescription(message).asException())
            } catch (e: ExecutionException) {
                val message = formatMessage(tx, q, "DELETE failed due to execution error: ${e.message}")
                LOGGER.info(message)
                responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            } catch (e: Throwable) {
                val message = formatMessage(tx, q, "DELETE failed due an unhandled error: ${e.message}")
                LOGGER.error(message, e)
                responseObserver.onError(Status.UNKNOWN.withDescription(message).asException())
            }
        }
    } catch (e: TransactionException.TransactionNotFoundException) {
        val message = "Execution failed because transaction ${request.txId.value} could not be resumed."
        LOGGER.info(message)
        responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(message).asException())
    }

    /**
     * gRPC endpoint for handling INSERT queries.
     */
    override fun insert(request: CottontailGrpc.InsertMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = try {
        this.withTransactionContext(request.txId) { tx, q ->
            try {
                val totalDuration = measureTime {
                    /* Bind query and create logical plan. */
                    val bindTimedValue = measureTimedValue {
                        this.binder.parseAndBindInsert(request, tx)
                    }
                    LOGGER.info(formatMessage(tx, q, "Parsing & binding INSERT took ${bindTimedValue.duration}."))

                    /* Plan query and create execution plan. */
                    val planTimedValue = measureTimedValue {
                        val candidates = this.planner.plan(bindTimedValue.value)
                        val operator = candidates.minByOrNull { it.totalCost }!!.toOperator(this.manager)
                        SpoolerSinkOperator(operator, q, 0, responseObserver)
                    }
                    LOGGER.info(formatMessage(tx, q, "Planning INSERT took ${planTimedValue.duration}."))

                    /* Execute INSERT. */
                    tx.execute(planTimedValue.value)
                }

                /* Finalize invocation. */
                responseObserver.onCompleted()
                LOGGER.trace("Executed INSERT in $totalDuration.")
            } catch (e: QueryException.QuerySyntaxException) {
                val message = "INSERT failed due to syntax error: ${e.message}"
                LOGGER.info(message)
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException())
            } catch (e: QueryException.QueryBindException) {
                val message = "INSERT failed due to binding error: ${e.message}"
                LOGGER.info(message)
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException())
            } catch (e: QueryException.QueryPlannerException) {
                val message = formatMessage(tx, q, "INSERT failed because of an error during query planning: ${e.message}")
                LOGGER.info(message)
                responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            } catch (e: TransactionException.DeadlockException) {
                val message = "INSERT failed due to deadlock with other transaction: ${e.message}"
                LOGGER.info(message)
                responseObserver.onError(Status.ABORTED.withDescription(message).asException())
            } catch (e: ExecutionException) {
                val message = "INSERT failed due to execution error: ${e.message}"
                LOGGER.info(message)
                responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            } catch (e: Throwable) {
                val message = "INSERT failed due an unhandled error: ${e.message}"
                LOGGER.error(message, e)
                responseObserver.onError(Status.UNKNOWN.withDescription(message).asException())
            }
        }
    } catch (e: TransactionException.TransactionNotFoundException) {
        val message = "Execution failed because transaction ${request.txId.value} could not be resumed."
        LOGGER.info(message)
        responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(message).asException())
    }
}
