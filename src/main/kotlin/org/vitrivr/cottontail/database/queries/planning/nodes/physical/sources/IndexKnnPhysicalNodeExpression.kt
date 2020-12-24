package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.sources.EntityIndexScanOperator

/**
 * A [NodeExpression.PhysicalNodeExpression] that represents a kNN query via an [Index] that support kNN lookups
 *
 * @author Ralph Gasser
 * @version 1.1.2
 */
class IndexKnnPhysicalNodeExpression(val index: Index, val knn: KnnPredicate<*>) : NullaryPhysicalNodeExpression() {
    override val canBePartitioned: Boolean = false
    override val outputSize: Long = (this.knn.k * this.knn.query.size).toLong()
    override val cost: Cost = this.index.cost(this.knn)
    override fun copy() = IndexKnnPhysicalNodeExpression(this.index, this.knn)
    override fun toOperator(engine: TransactionManager): Operator = EntityIndexScanOperator(this.index, this.knn)
    override fun partition(p: Int): List<NullaryPhysicalNodeExpression> {
        /* TODO: May actually be possible for certain index structures. */
        throw IllegalStateException("IndexKnnPhysicalNodeExpression cannot be partitioned.")
    }
}