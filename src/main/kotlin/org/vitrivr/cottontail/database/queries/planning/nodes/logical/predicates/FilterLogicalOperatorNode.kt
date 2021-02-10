package org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate

/**
 * A [LogicalOperatorNode] that formalizes filtering using some [BooleanPredicate].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class FilterLogicalOperatorNode(val predicate: BooleanPredicate) : UnaryLogicalOperatorNode() {

    /** The [FilterLogicalOperatorNode] returns the [ColumnDef] of its input, or no column at all. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input?.columns ?: emptyArray()

    /**
     * Returns a copy of this [KnnLogicalOperatorNode]
     *
     * @return Copy of this [KnnLogicalOperatorNode]
     */
    override fun copy(): FilterLogicalOperatorNode = FilterLogicalOperatorNode(this.predicate)

    /**
     * Calculates and returns the digest for this [FilterLogicalOperatorNode].
     *
     * @return Digest for this [FilterLogicalOperatorNode]
     */
    override fun digest(): Long = 31L * super.digest() + this.predicate.digest()
}