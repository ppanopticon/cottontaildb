package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.queries.planning.CottontailQueryPlanner
import org.vitrivr.cottontail.database.queries.planning.rules.logical.DeferredFetchAfterKnnRewriteRule
import org.vitrivr.cottontail.database.queries.planning.rules.logical.LeftConjunctionRewriteRule
import org.vitrivr.cottontail.database.queries.planning.rules.logical.RightConjunctionRewriteRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation.*
import org.vitrivr.cottontail.database.queries.planning.rules.physical.index.BooleanIndexScanRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.pushdown.CountPushdownRule
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.exceptions.ExecutionException
import org.vitrivr.cottontail.grpc.CottonDQLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.server.grpc.helper.GrpcQueryBinder
import org.vitrivr.cottontail.server.grpc.helper.ResultsSpoolerOperator
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime
import kotlin.time.measureTimedValue

/**
 * Implementation of [CottonDQLGrpc.CottonDQLImplBase], the gRPC endpoint for querying data in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.3
 */
@ExperimentalTime
class CottonDQLService(val catalogue: Catalogue, val engine: ExecutionEngine) : CottonDQLGrpc.CottonDQLImplBase() {


    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(CottonDQLService::class.java)
    }

    /** [GrpcQueryBinder] used to generate [org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalNodeExpression] tree from a gRPC query. */
    private val binder = GrpcQueryBinder(catalogue = this@CottonDQLService.catalogue)

    /** [CottontailQueryPlanner] used to generate execution plans from query definitions. */
    private val planner = CottontailQueryPlanner(
            logicalRewriteRules = listOf(
                    LeftConjunctionRewriteRule,
                    RightConjunctionRewriteRule,
                    DeferredFetchAfterKnnRewriteRule
            ),
            physicalRewriteRules = listOf(
                    BooleanIndexScanRule,
                    CountPushdownRule,
                    EntityScanImplementationRule,
                    FilterImplementationRule,
                    KnnImplementationRule,
                    LimitImplementationRule,
                    ProjectionImplementationRule,
                    FetchImplementationRule
            )
    )

    /**
     * gRPC endpoint for handling simple queries.
     */
    override fun query(request: CottontailGrpc.QueryMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = try {
        /* Create a new execution context for the query. */
        val context = this.engine.ExecutionContext()
        val queryId = request.queryId.ifBlank { context.uuid.toString() }
        val totalDuration = measureTime {
            /* Bind query and create logical plan. */
            val bindTimedValue = measureTimedValue {
                this.binder.parseAndBind(request.query)
            }
            LOGGER.trace("Parsing & binding query $queryId took ${bindTimedValue.duration}.")

            /* Plan query and create execution plan. */
            val planningTime = measureTime {
                val candidates = this.planner.plan(bindTimedValue.value)
                if (candidates.isEmpty()) {
                    responseObserver.onError(Status.INTERNAL.withDescription("Query execution failed because no valid execution plan could be produced").asException())
                    return
                }
                val operator = candidates.minByOrNull { it.totalCost }!!.toOperator(context)
                context.addOperator(ResultsSpoolerOperator(operator, context, queryId, 0, responseObserver))
            }
            LOGGER.trace("Planning query $queryId took $planningTime.")

            /* Execute query. */
            context.execute()
        }

        /* Complete query. */
        responseObserver.onCompleted()
        LOGGER.trace("Executing query ${context.uuid} took $totalDuration to complete.")
    } catch (e: QueryException.QuerySyntaxException) {
        LOGGER.error("Error while executing query $request", e)
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Query syntax is invalid: ${e.message}").asException())
    } catch (e: QueryException.QueryBindException) {
        LOGGER.error("Error while executing query $request", e)
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Query binding failed: ${e.message}").asException())
    } catch (e: ExecutionException) {
        LOGGER.error("Error while executing query $request", e)
        responseObserver.onError(Status.INTERNAL.withDescription("Query execution failed: ${e.message}").asException())
    } catch (e: DatabaseException) {
        LOGGER.error("Error while executing query $request", e)
        responseObserver.onError(Status.INTERNAL.withDescription("Query execution failed failed because of a database error: ${e.message}").asException())
    } catch (e: Throwable) {
        LOGGER.error("Error while executing query $request", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Query execution failed failed because of an unknown error: ${e.message}").asException())
    }

    /**
     *  gRPC endpoint for handling batched queries.
     */
    override fun batchedQuery(request: CottontailGrpc.BatchedQueryMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = try {
        /* Create a new execution context for the query. */
        val context = this.engine.ExecutionContext()
        val queryId = request.queryId.ifBlank { context.uuid.toString() }

        val totalDuration = measureTime {
            request.queriesList.forEachIndexed { index, query ->
                /* Bind query and create logical plan. */
                val bindTimedValue = measureTimedValue {
                    this.binder.parseAndBind(query)
                }
                LOGGER.trace("Parsing & binding query: $queryId, index: $index took ${bindTimedValue.duration}.")

                /* Plan query and create execution plan. */
                val planTimedValue = measureTimedValue {
                    val candidates = this.planner.plan(bindTimedValue.value)
                    val operator = candidates.minByOrNull { it.totalCost }!!.toOperator(context)
                    context.addOperator(ResultsSpoolerOperator(operator, context, queryId, index, responseObserver))
                }
                LOGGER.trace("Planning query: $queryId, index: $index took ${planTimedValue.duration}.")

                /* Schedule query for execution. */
                context.execute()
            }
        }

        /* Complete query. */
        responseObserver.onCompleted()
        LOGGER.info("Executing batched query $queryId took $totalDuration to complete.")
    } catch (e: QueryException.QuerySyntaxException) {
        LOGGER.error("Error while executing batched query $request", e)
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Query syntax is invalid: ${e.message}").asException())
    } catch (e: QueryException.QueryBindException) {
        LOGGER.error("Error while executing batched query $request", e)
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Query binding failed: ${e.message}").asException())
    } catch (e: ExecutionException) {
        LOGGER.error("Error while executing query $request", e)
        responseObserver.onError(Status.INTERNAL.withDescription("Query execution failed: ${e.message}").asException())
    } catch (e: DatabaseException) {
        LOGGER.error("Error while executing query $request", e)
        responseObserver.onError(Status.INTERNAL.withDescription("Query execution failed failed because of a database error: ${e.message}").asException())
    } catch (e: Throwable) {
        LOGGER.error("Error while executing batched query $request", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("Query execution failed failed because of a unknown error: ${e.message}").asException())
    }

    /**
     * gRPC endpoint for handling PING requests.
     */
    override fun ping(request: CottontailGrpc.Empty, responseObserver: StreamObserver<CottontailGrpc.Status>) {
        responseObserver.onNext(CottontailGrpc.Status.newBuilder().setSuccess(true).setTimestamp(System.currentTimeMillis()).build())
        responseObserver.onCompleted()
    }
}
