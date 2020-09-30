package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator

/**
 * A [NodeExpression.PhysicalNodeExpression] that represents a kNN query via an [Index] that support kNN lookups
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class IndexKnnPhysicalNodeExpression(val entity: Entity, val knn: KnnPredicate<*>, val index: Index) : NullaryPhysicalNodeExpression() {
    override val canBePartitioned: Boolean = false
    override val outputSize: Long = (this.knn.k * this.knn.query.size).toLong()
    override val cost: Cost = this.index.cost(this.knn)
    override fun copy() = IndexKnnPhysicalNodeExpression(this.entity, this.knn, this.index)
    override fun toOperator(context: ExecutionEngine.ExecutionContext): Operator {
        TODO("Not yet implemented")
    }

    override fun partition(p: Int): List<NullaryPhysicalNodeExpression> {
        /* TODO: May actually be possible for certain index structures. */
        throw IllegalStateException("IndexKnnPhysicalNodeExpression cannot be partitioned.")
    }
}