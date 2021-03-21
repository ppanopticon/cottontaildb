package org.vitrivr.cottontail.server.grpc.services

import com.google.protobuf.Empty
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.GrpcQueryBinder
import org.vitrivr.cottontail.database.queries.planning.CottontailQueryPlanner
import org.vitrivr.cottontail.database.queries.planning.rules.logical.*
import org.vitrivr.cottontail.database.queries.planning.rules.physical.index.BooleanIndexScanRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.index.KnnIndexScanRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.merge.LimitingSortMergeRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.pushdown.CountPushdownRule
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.operators.sinks.SpoolerSinkOperator
import org.vitrivr.cottontail.execution.operators.system.ExplainQueryOperator
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DQLGrpc
import org.vitrivr.cottontail.model.exceptions.ExecutionException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.exceptions.TransactionException
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Implementation of [DQLGrpc.DQLImplBase], the gRPC endpoint for querying data in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
@ExperimentalTime
class DQLService(val catalogue: Catalogue, override val manager: TransactionManager) : DQLGrpc.DQLImplBase(), TransactionService {

    /** [GrpcQueryBinder] used to generate a logical query plan. */
    private val binder = GrpcQueryBinder(catalogue = this@DQLService.catalogue)

    /** [CottontailQueryPlanner] used to generate execution plans from a logical query plan. */
    private val planner = CottontailQueryPlanner(
        logicalRules = listOf(
            LeftConjunctionRewriteRule,
            RightConjunctionRewriteRule,
            LeftConjunctionOnSubselectRewriteRule,
            RightConjunctionOnSubselectRewriteRule,
            DeferFetchOnScanRewriteRule,
            DeferFetchOnFetchRewriteRule
        ),
        physicalRules = listOf(BooleanIndexScanRule, KnnIndexScanRule, CountPushdownRule, LimitingSortMergeRule),
        this.catalogue.config.cache.planCacheSize
    )

    /**
     * gRPC endpoint for executing queries.
     */
    override fun query(request: CottontailGrpc.QueryMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId, responseObserver) { tx, q ->
        try {
            /* Start query execution. */
            val ctx = QueryContext(tx)
            val totalDuration = measureTime {
                /* Bind query and create logical plan. */
                this.binder.bind(request.query, ctx)

                /* Plan query and create execution plan. */
                this.planner.planAndSelect(ctx)

                /* Execute query in transaction context. */
                tx.execute(SpoolerSinkOperator(ctx.toOperatorTree(tx), q, 0, responseObserver))
            }

            /* Finalize transaction. */
            Status.OK.withDescription(formatMessage(tx, q, "Executing query took ${totalDuration}."))
        } catch (e: QueryException.QuerySyntaxException) {
            Status.INVALID_ARGUMENT.withDescription(formatMessage(tx, q, "Could not execute query due to syntax error: ${e.message}"))
        } catch (e: QueryException.QueryBindException) {
            Status.INVALID_ARGUMENT.withDescription(formatMessage(tx, q, "Could not execute query because DBO could not be found: ${e.message}"))
        } catch (e: TransactionException.DeadlockException) {
            Status.ABORTED.withDescription(formatMessage(tx, q, "Could not execute query due to deadlock with other transaction: ${e.message}"))
        } catch (e: QueryException.QueryPlannerException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Could not execute query because of an error during query planning: ${e.message}")).withCause(e)
        } catch (e: ExecutionException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Could not execute query due to an unhandled execution error: ${e.message}")).withCause(e)
        } catch (e: Throwable) {
            Status.UNKNOWN.withDescription(formatMessage(tx, q, "Could not execute query due to an unhandled error: ${e.message}")).withCause(e)
        }
    }

    /**
     * gRPC endpoint for explaining queries.
     */
    override fun explain(request: CottontailGrpc.QueryMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId, responseObserver) { tx, q ->
        try {
            /* Start query execution. */
            val ctx = QueryContext(tx)
            val totalDuration = measureTime {
                /* Bind query and create logical plan. */
                this.binder.bind(request.query, ctx)

                /* Plan query and create execution plan candidates. */
                val candidates = this.planner.plan(ctx)

                /* Print execution plans. */
                tx.execute(SpoolerSinkOperator(ExplainQueryOperator(candidates), q, 0, responseObserver))
            }

            Status.OK.withDescription(formatMessage(tx, q, "Explaining query took ${totalDuration}."))
        } catch (e: QueryException.QuerySyntaxException) {
            Status.INVALID_ARGUMENT.withDescription(formatMessage(tx, q, "Could not explain query due to syntax error: ${e.message}"))
        } catch (e: QueryException.QueryBindException) {
            Status.INVALID_ARGUMENT.withDescription(formatMessage(tx, q, "Could not explain query because DBO could not be found: ${e.message}"))
        } catch (e: TransactionException.DeadlockException) {
            Status.ABORTED.withDescription(formatMessage(tx, q, "Could not explain query due to deadlock with other transaction: ${e.message}"))
        } catch (e: QueryException.QueryPlannerException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Could not execute query because of an error during query planning: ${e.message}")).withCause(e)
        } catch (e: ExecutionException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "Could not explain query due to an unhandled execution error: ${e.message}")).withCause(e)
        } catch (e: Throwable) {
            Status.UNKNOWN.withDescription(formatMessage(tx, q, "Could not explain query due to an unhandled error: ${e.message}")).withCause(e)
        }
    }

    /**
     * gRPC endpoint for handling PING requests.
     */
    override fun ping(request: Empty, responseObserver: StreamObserver<Empty>) {
        responseObserver.onNext(Empty.getDefaultInstance())
        responseObserver.onCompleted()
    }
}
