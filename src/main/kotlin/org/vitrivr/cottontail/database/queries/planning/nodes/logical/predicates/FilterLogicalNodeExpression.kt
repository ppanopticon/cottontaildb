package org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates

import org.vitrivr.cottontail.database.queries.binding.BooleanPredicateBinding
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [LogicalNodeExpression] that formalizes filtering using some [BooleanPredicate].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class FilterLogicalNodeExpression(val predicate: BooleanPredicateBinding) :
    UnaryLogicalNodeExpression() {

    /** The [FilterLogicalNodeExpression] returns the [ColumnDef] of its input, or no column at all. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input?.columns ?: emptyArray()

    /**
     * Returns a copy of this [KnnLogicalNodeExpression]
     *
     * @return Copy of this [KnnLogicalNodeExpression]
     */
    override fun copy(): FilterLogicalNodeExpression = FilterLogicalNodeExpression(this.predicate)

    /**
     * Calculates and returns the digest for this [FilterLogicalNodeExpression].
     *
     * @return Digest for this [FilterLogicalNodeExpression]
     */
    override fun digest(): Long = 31L * super.digest() + this.predicate.hashCode()
}