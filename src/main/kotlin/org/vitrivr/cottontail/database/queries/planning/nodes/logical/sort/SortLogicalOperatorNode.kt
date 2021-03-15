package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sort

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sort.SortPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * A [UnaryLogicalOperatorNode] that represents sorting the input by a set of specified [ColumnDef]s.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class SortLogicalOperatorNode(input: OperatorNode.Logical? = null, sortOn: Array<Pair<ColumnDef<*>, SortOrder>>) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Order"
    }

    /** The name of this [SortLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [SortLogicalOperatorNode] returns the [ColumnDef] of its input. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input?.columns ?: emptyArray()

    /** A [SortLogicalOperatorNode] orders the input in by the specified [ColumnDef]s. */
    override val order = sortOn

    /** The [SortLogicalOperatorNode] requires all [ColumnDef]s used in the [KnnPredicate]. */
    override val requires: Array<ColumnDef<*>> = this.order.map { it.first }.toTypedArray()

    init {
        /* Sanity check. */
        if (sortOn.isEmpty()) throw QueryException.QuerySyntaxException("At least one column must be specified for sorting.")
    }

    /**
     * Creates and returns a copy of this [SortLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [SortLogicalOperatorNode].
     */
    override fun copy() = SortLogicalOperatorNode(sortOn = this.order)

    /**
     * Returns a [SortPhysicalOperatorNode] representation of this [SortLogicalOperatorNode]
     *
     * @return [SortPhysicalOperatorNode]
     */
    override fun implement(): Physical = SortPhysicalOperatorNode(this.input?.implement(), this.order)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SortLogicalOperatorNode) return false

        if (!order.contentEquals(other.order)) return false

        return true
    }

    /** Generates and returns a hash code for this [SortLogicalOperatorNode]. */
    override fun hashCode(): Int = this.order.contentHashCode()

    /** Generates and returns a [String] representation of this [SortLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.order.joinToString(",") { "${it.first.name} ${it.second}" }}]"
}