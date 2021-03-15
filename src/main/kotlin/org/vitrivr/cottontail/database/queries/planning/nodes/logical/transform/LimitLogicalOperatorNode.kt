package org.vitrivr.cottontail.database.queries.planning.nodes.logical.transform

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.transform.LimitPhysicalOperatorNode

/**
 * A [UnaryLogicalOperatorNode] that represents the application of a LIMIT and/or SKIP clause on the final result.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class LimitLogicalOperatorNode(input: Logical? = null, val limit: Long, val skip: Long) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Limit"
    }

    /** The name of this [LimitLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [LimitLogicalOperatorNode] returns the [ColumnDef] of its input, or no column at all. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input?.columns ?: emptyArray()

    init {
        require(this.limit > 0) { "Limit must be greater than zero but isn't (limit = $limit)." }
        require(this.skip >= 0) { "Skip must be greater or equal to zero but isn't (limit = $skip)." }
    }

    /**
     * Creates and returns a copy of this [LimitLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [LimitLogicalOperatorNode].
     */
    override fun copy() = LimitLogicalOperatorNode(limit = this.limit, skip = this.skip)

    /**
     * Returns a [LimitPhysicalOperatorNode] representation of this [LimitLogicalOperatorNode]
     *
     * @return [LimitPhysicalOperatorNode]
     */
    override fun implement(): Physical = LimitPhysicalOperatorNode(this.input?.implement(), this.limit, this.skip)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is LimitLogicalOperatorNode) return false

        if (limit != other.limit) return false
        if (skip != other.skip) return false

        return true
    }

    /** Generates and returns a hash code for this [LimitLogicalOperatorNode]. */
    override fun hashCode(): Int {
        var result = limit.hashCode()
        result = 27 * result + skip.hashCode()
        return result
    }

    /** Generates and returns a [String] representation of this [LimitLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.skip},${this.limit}]"
}