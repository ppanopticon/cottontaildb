package ch.unibas.dmi.dbis.cottontail.execution

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.KnnPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.Projection
import ch.unibas.dmi.dbis.cottontail.database.queries.ProjectionType
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionStage
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.boolean.EntityIndexedFilterTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.boolean.EntityLinearScanFilterTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.fetch.EntityFetchColumnsTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn.BooleanIndexedKnnTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn.EntityIndexedKnnTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn.LinearEntityScanKnnTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn.ParallelEntityScanKnnTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.projection.*
import ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection.*
import ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.transform.RecordsetLimitTask
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import kotlin.math.min


/**
 *
 */
class ExecutionPlanFactory (val executionEngine: ExecutionEngine) {


    companion object {
        /** Threshold under which parallelism starts to kick in. TODO: Find optimal value experimentally. */
        private const val KNN_OP_PARALLELISM_THRESHOLD = 819200000L
    }

    /**
     * Returns an [ExecutionPlan] for the specified, simple query that does not contain any JOINS.
     *
     * @param entity The [Entity] from which to fetch the data.
     * @param projectionClause The [Projection] clause of the query.
     * @param whereClause The [BooleanPredicate] (WHERE) clause of the query.
     * @param knnClause The [KnnPredicate] (KNN) clause of the query.
     *
     * @return The resulting [ExecutionPlan]
     */
    fun simpleExecutionPlan(entity: Entity, projectionClause: Projection, whereClause: BooleanPredicate? = null, knnClause: KnnPredicate<*>? = null, limit: Long = -1, skip: Long = -1): ExecutionPlan {
        val plan = this.executionEngine.newExecutionPlan()

        /* Add kNN and/or WHERE clause. */
        var last: String = if (whereClause == null && knnClause == null) {
            when (projectionClause.type) {
                ProjectionType.SELECT -> {
                    val internal = plan.addTask(ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.boolean.EntityLinearScanTask(entity, projectionClause.columns))
                    plan.addTask(RecordsetSelectProjectionTask(projectionClause), internal)
                }
                ProjectionType.COUNT ->  plan.addTask(EntityCountProjectionTask(entity))
                ProjectionType.EXISTS -> plan.addTask(EntityExistsProjectionTask(entity))
                ProjectionType.SUM -> plan.addTask(EntitySumProjectionTask(entity, projectionClause.columns.first()))
                ProjectionType.MAX -> plan.addTask(EntityMaxProjectionTask(entity, projectionClause.columns.first()))
                ProjectionType.MIN -> plan.addTask(EntityMinProjectionTask(entity, projectionClause.columns.first()))
                ProjectionType.MEAN -> plan.addTask(EntityMeanProjectionTask(entity, projectionClause.columns.first()))
            }
            return plan
        } else if (knnClause != null && whereClause == null) {
            plan.addStage(planAndLayoutSimpleKnn(entity, knnClause))
        } else if (whereClause != null && knnClause == null) {
            plan.addStage(planAndLayoutWhere(entity, whereClause))
        } else {
            plan.addStage(planAndLayoutCombinedKnn(entity, knnClause!!, whereClause!!))
        }

        /* Add SELECT clause (column fetching + projection) */
        last = when (projectionClause.type) {
            ProjectionType.SELECT -> {
                val internal = plan.addTask(EntityFetchColumnsTask(entity, projectionClause.columns), last)
                plan.addTask(RecordsetSelectProjectionTask(projectionClause), internal)
            }
            ProjectionType.SUM -> {
                val internal = plan.addTask(EntityFetchColumnsTask(entity, projectionClause.columns), last)
                plan.addTask(RecordsetSumProjectionTask(projectionClause), internal)
            }
            ProjectionType.MAX -> {
                val internal = plan.addTask(EntityFetchColumnsTask(entity, projectionClause.columns), last)
                plan.addTask(RecordsetMaxProjectionTask(projectionClause), internal)
            }
            ProjectionType.MIN -> {
                val internal = plan.addTask(EntityFetchColumnsTask(entity, projectionClause.columns), last)
                plan.addTask(RecordsetMinProjectionTask(projectionClause), internal)
            }
            ProjectionType.MEAN -> {
                val internal = plan.addTask(EntityFetchColumnsTask(entity, projectionClause.columns), last)
                plan.addTask(RecordsetMeanProjectionTask(projectionClause), internal)
            }
            ProjectionType.COUNT -> {
                plan.addTask(RecordsetCountProjectionTask(), last)
            }
            ProjectionType.EXISTS -> {
                plan.addTask(RecordsetExistsProjectionTask(), last)
            }
        }

        /* TODO: ORDER BY clause. */

        /* Add LIMIT clause (if required). */
        if (limit > 0 || skip > 0) {
            plan.addTask(RecordsetLimitTask(limit, skip), last)
        }

        return plan
    }

    /**
     * Plans different execution paths for the given [KnnPredicate] and [BooleanPredicate] and returns the most efficient one in terms of cost.
     *
     * @param entity [Entity] on which to execute the [KnnPredicate]
     * @param knnClause The [KnnPredicate] to execute.
     * @param whereClause The [BooleanPredicate] to execute.
     * @return [ExecutionStage] that is expected to be most efficient in terms of costs.
     */
    private fun planAndLayoutCombinedKnn(entity: Entity, knnClause: KnnPredicate<*>, whereClause: BooleanPredicate): ExecutionStage {
        /* Generate empty list of execution branches. */
        val candidates = mutableListOf<ExecutionStage>()

        /* Add default case 1: Full table scan based Knn. */
        val operations = whereClause.cost * entity.statistics.rows + knnClause.query.first().logicalSize * entity.statistics.rows * knnClause.cost
        val parallelism = min((operations / KNN_OP_PARALLELISM_THRESHOLD).toInt(), Runtime.getRuntime().availableProcessors() / 2).toShort()
        val stage = ExecutionStage()
        val task = if (parallelism > 1) {
            ParallelEntityScanKnnTask(entity, knnClause, whereClause, parallelism)
        } else {
            LinearEntityScanKnnTask(entity, knnClause, whereClause)
        }
        stage.addTask(task)
        candidates.add(stage)


        /* Add default case 2: Cheapest index for full query. */
        val indexes = entity.allIndexes()
        val index = indexes.filter { it.canProcess(whereClause) }.minBy { it.cost(whereClause) }
        if (index != null) {
            val stage = ExecutionStage()
            stage.addTask(BooleanIndexedKnnTask(entity,knnClause, whereClause, index))
            candidates.add(stage)
        }

        /* TODO: Try other paths by re-arranging whereClause */

        /* Take cheapest execution path and return it. */
        candidates.sortBy { it.cost }
        return candidates.first()
    }

    /**
     * Plans different execution paths for the given [KnnPredicate] and returns the most efficient one in terms of cost.
     *
     * @param entity [Entity] on which to execute the [KnnPredicate]
     * @param knnClause The [KnnPredicate] to execute.
     * @return [ExecutionStage] that is expected to be most efficient in terms of costs.
     */
    private fun planAndLayoutSimpleKnn(entity: Entity, knnClause: KnnPredicate<*>): ExecutionStage {
        /* Generate empty list of execution branches. */
        val candidates = mutableListOf<ExecutionStage>()

        /* Add default case 1: Full table scan based Knn. */
        val operations = knnClause.query.first().logicalSize * entity.statistics.rows * knnClause.cost
        val parallelism = min((operations/KNN_OP_PARALLELISM_THRESHOLD).toInt(), Runtime.getRuntime().availableProcessors() / 2).toShort()
        var stage = ExecutionStage()
        val task = if (parallelism > 1) {
            ParallelEntityScanKnnTask(entity, knnClause, null, parallelism)
        } else {
            LinearEntityScanKnnTask(entity, knnClause, null)
        }
        stage.addTask(task)
        candidates.add(stage)

        /* Add default case 2: Cheapest index for full query. */
        val index = entity.allIndexes().filter {
            if (knnClause.inexact) {
                it.canProcess(knnClause)
            } else {
                !it.type.inexact && it.canProcess(knnClause)
            }
        }.minBy { it.cost(knnClause) }
        if (index != null) {
            stage = ExecutionStage()
            stage.addTask(EntityIndexedKnnTask(entity, knnClause, index))
            candidates.add(stage)
        }

        /* Make sure that list of candidates contains elements. */
        if (candidates.size == 0) {
            throw QueryException.QueryBindException("Failed to generate a valid execution plan; no path found to satisfy kNN-clause.")
        }

        /* Take cheapest ExecutionPlan and return it. */
        candidates.sortBy { it.cost }
        return candidates.first()
    }

    /**
     * Plans different execution paths for the [BooleanPredicate] and returns the most efficient one in terms of cost.
     *
     * @param entity [Entity] on which to execute the [BooleanPredicate]
     * @param whereClause The [BooleanPredicate] to execute.
     * @return [ExecutionStage] that is expected to be most efficient in terms of costs.
     */
    private fun planAndLayoutWhere(entity: Entity, whereClause: BooleanPredicate): ExecutionStage {

        /* Generate empty list of execution branches. */
        val candidates = mutableListOf<ExecutionStage>()

        /* Add default case 1: Full table scan. */
        if (entity.canProcess(whereClause)) {
            val stage = ExecutionStage()
            stage.addTask(EntityLinearScanFilterTask(entity, whereClause))
            candidates.add(stage)
        }

        /* Add default case 2: Cheapest index for full query. */
        val indexes = entity.allIndexes()
        val index = indexes.filter { it.canProcess(whereClause) }.minBy { it.cost(whereClause) }
        if (index != null) {
            val stage = ExecutionStage()
            stage.addTask(EntityIndexedFilterTask(entity, whereClause, index))
            candidates.add(stage)
        }

        /* TODO: Try other paths by re-arranging whereClause */

        /* Check if list of candidates contains elements. */
        if (candidates.size == 0) {
            throw QueryException.QueryBindException("Failed to generate a valid execution plan; no path found to satisfy WHERE-clause.")
        }

        /* Take cheapest execution path and return it. */
        candidates.sortBy { it.cost }
        return candidates.first()
    }
}


