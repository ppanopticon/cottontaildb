package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.sources.EntityIndexScanOperator

/**
 * A [IndexScanPhysicalOperatorNode] that represents a predicated lookup using an [Index].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class IndexScanPhysicalOperatorNode(val index: Index, val predicate: BooleanPredicate) :
    NullaryPhysicalOperatorNode() {
    val selectivity: Float = Cost.COST_DEFAULT_SELECTIVITY
    override val columns: Array<ColumnDef<*>>
        get() = this.index.produces
    override val canBePartitioned: Boolean = false
    override val outputSize: Long = (this.index.parent.statistics.rows * this.selectivity).toLong()
    override val cost: Cost = this.index.cost(this.predicate)
    override fun copy() = IndexScanPhysicalOperatorNode(this.index, this.predicate)

    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator =
        EntityIndexScanOperator(this.index, this.predicate.bindValues(ctx))

    override fun bindValues(ctx: QueryContext): OperatorNode {
        this.predicate.bindValues(ctx)
        return super.bindValues(ctx)
    }

    override fun partition(p: Int): List<NullaryPhysicalOperatorNode> {
        /* TODO: May actually be possible for certain index structures. */
        throw IllegalStateException("IndexScanPhysicalNodeExpression cannot be partitioned.")
    }

    /**
     * Calculates and returns the digest for this [IndexScanPhysicalOperatorNode].
     *
     * @return Digest for this [IndexScanPhysicalOperatorNode]e
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.predicate.digest()
        result = 31L * result + this.index.hashCode()
        return result
    }
}