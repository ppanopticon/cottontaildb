package org.vitrivr.cottontail.server.grpc.services

import com.google.protobuf.Empty
import kotlinx.coroutines.flow.Flow
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
import org.vitrivr.cottontail.execution.operators.system.ExplainQueryOperator
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DQLGrpc
import org.vitrivr.cottontail.grpc.DQLGrpcKt
import kotlin.time.ExperimentalTime

/**
 * Implementation of [DQLGrpc.DQLImplBase], the gRPC endpoint for querying data in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@ExperimentalTime
class DQLService(val catalogue: Catalogue, override val manager: TransactionManager) : DQLGrpcKt.DQLCoroutineImplBase(), gRPCTransactionService {

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
    override fun query(request: CottontailGrpc.QueryMessage): Flow<CottontailGrpc.QueryResponseMessage> = this.withTransactionContext(request.txId, "EXECUTE QUERY") { tx, q ->
        /* Start query execution. */
        val ctx = QueryContext(tx)

        /* Bind query and create logical plan. */
        this.binder.bind(request.query, ctx)

        /* Plan query and create execution plan. */
        this.planner.planAndSelect(ctx)

        /* Execute query in transaction context. */
        executeAndMaterialize(tx, ctx.toOperatorTree(tx), q, 0)
    }

    /**
     * gRPC endpoint for explaining queries.
     */
    override fun explain(request: CottontailGrpc.QueryMessage): Flow<CottontailGrpc.QueryResponseMessage> = this.withTransactionContext(request.txId, "EXPLAIN QUERY") { tx, q ->
        val ctx = QueryContext(tx)

        /* Bind query and create logical plan. */
        this.binder.bind(request.query, ctx)

        /* Plan query and create execution plan candidates. */
        val candidates = this.planner.plan(ctx)

        /* Return execution plans. */
        executeAndMaterialize(tx, ExplainQueryOperator(candidates), q, 0)
    }

    /**
     * gRPC endpoint for handling PING requests.
     */
    override suspend fun ping(request: Empty): Empty {
        return Empty.getDefaultInstance()
    }
}
