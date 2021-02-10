package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.KnnPredicateBinding
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.sources.EntityIndexScanOperator
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [NodeExpression.PhysicalNodeExpression] that represents a kNN query via an [Index] that support kNN lookups
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class IndexKnnPhysicalNodeExpression(val index: Index, val knn: KnnPredicateBinding) :
    NullaryPhysicalNodeExpression() {
    override val columns: Array<ColumnDef<*>>
        get() = this.index.produces
    override val canBePartitioned: Boolean = false
    override val outputSize: Long = (this.knn.k * this.knn.query.size).toLong()
    override val cost: Cost = this.index.cost(this.knn)
    override fun copy() = IndexKnnPhysicalNodeExpression(this.index, this.knn)
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator =
        EntityIndexScanOperator(this.index, this.knn.apply(ctx))

    override fun partition(p: Int): List<NullaryPhysicalNodeExpression> {
        /* TODO: May actually be possible for certain index structures. */
        throw IllegalStateException("IndexKnnPhysicalNodeExpression cannot be partitioned.")
    }

    /**
     * Calculates and returns the digest for this [IndexKnnPhysicalNodeExpression].
     *
     * @return Digest for this [IndexKnnPhysicalNodeExpression]e
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.index.hashCode()
        result = 31L * result + this.knn.hashCode()
        return result
    }
}