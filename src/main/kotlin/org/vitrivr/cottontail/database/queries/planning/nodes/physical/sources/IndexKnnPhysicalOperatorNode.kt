package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.sources.EntityIndexScanOperator

/**
 * A [NodeExpression.PhysicalNodeExpression] that represents a kNN query via an [Index] that support kNN lookups
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class IndexKnnPhysicalOperatorNode(val index: Index, val predicate: KnnPredicate) :
    NullaryPhysicalOperatorNode() {
    override val columns: Array<ColumnDef<*>>
        get() = this.index.produces
    override val canBePartitioned: Boolean = false
    override val outputSize: Long = (this.predicate.k * this.predicate.query.size).toLong()
    override val cost: Cost = this.index.cost(this.predicate)
    override fun copy() = IndexKnnPhysicalOperatorNode(this.index, this.predicate)

    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator =
        EntityIndexScanOperator(this.index, this.predicate.bindValues(ctx))

    override fun bindValues(ctx: QueryContext): OperatorNode {
        this.predicate.bindValues(ctx)
        return super.bindValues(ctx)
    }

    override fun partition(p: Int): List<NullaryPhysicalOperatorNode> {
        /* TODO: May actually be possible for certain index structures. */
        throw IllegalStateException("IndexKnnPhysicalNodeExpression cannot be partitioned.")
    }

    /**
     * Calculates and returns the digest for this [IndexKnnPhysicalOperatorNode].
     *
     * @return Digest for this [IndexKnnPhysicalOperatorNode]e
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.predicate.digest()
        result = 31L * result + this.index.hashCode()
        return result
    }
}