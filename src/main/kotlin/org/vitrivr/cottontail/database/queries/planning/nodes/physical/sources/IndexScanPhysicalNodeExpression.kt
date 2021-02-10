package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.BooleanPredicateBinding
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.sources.EntityIndexScanOperator
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [IndexScanPhysicalNodeExpression] that represents a predicated lookup using an [Index].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class IndexScanPhysicalNodeExpression(val index: Index, val predicate: BooleanPredicateBinding) :
    NullaryPhysicalNodeExpression() {
    val selectivity: Float = Cost.COST_DEFAULT_SELECTIVITY
    override val columns: Array<ColumnDef<*>>
        get() = this.index.produces
    override val canBePartitioned: Boolean = false
    override val outputSize: Long = (this.index.parent.statistics.rows * this.selectivity).toLong()
    override val cost: Cost = this.index.cost(this.predicate)
    override fun copy() = IndexScanPhysicalNodeExpression(this.index, this.predicate)
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator =
        EntityIndexScanOperator(this.index, this.predicate.apply(ctx))

    override fun partition(p: Int): List<NullaryPhysicalNodeExpression> {
        /* TODO: May actually be possible for certain index structures. */
        throw IllegalStateException("IndexScanPhysicalNodeExpression cannot be partitioned.")
    }

    /**
     * Calculates and returns the digest for this [IndexScanPhysicalNodeExpression].
     *
     * @return Digest for this [IndexScanPhysicalNodeExpression]e
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.index.hashCode()
        result = 31L * result + this.predicate.hashCode()
        return result
    }
}