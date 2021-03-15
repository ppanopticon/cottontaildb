package org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates.FilterPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate

/**
 * A [UnaryLogicalOperatorNode] that formalizes filtering using some [BooleanPredicate].
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class FilterLogicalOperatorNode(input: Logical? = null, val predicate: BooleanPredicate) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Filter"
    }

    /** The name of this [FilterLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [FilterLogicalOperatorNode] returns the [ColumnDef] of its input, or no column at all. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input?.columns ?: emptyArray()

    /** The [FilterLogicalOperatorNode] requires all [ColumnDef]s used in the [BooleanPredicate]. */
    override val requires: Array<ColumnDef<*>>
        get() = this.predicate.columns.toTypedArray()

    /**
     * Creates and returns a copy of this [FilterLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [FilterLogicalOperatorNode].
     */
    override fun copy() = FilterLogicalOperatorNode(predicate = this.predicate)

    /**
     * Returns a [FilterPhysicalOperatorNode] representation of this [FilterLogicalOperatorNode]
     *
     * @return [FilterPhysicalOperatorNode]
     */
    override fun implement(): Physical = FilterPhysicalOperatorNode(this.input?.implement(), this.predicate)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FilterLogicalOperatorNode) return false
        if (this.predicate != other.predicate) return false
        return true
    }

    /** Generates and returns a hash code for this [FilterLogicalOperatorNode]. */
    override fun hashCode(): Int = this.predicate.hashCode()

    /** Generates and returns a [String] representation of this [FilterLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.predicate}]"
}