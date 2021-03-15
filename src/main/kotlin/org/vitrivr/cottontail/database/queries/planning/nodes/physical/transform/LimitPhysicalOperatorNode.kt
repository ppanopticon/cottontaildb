package org.vitrivr.cottontail.database.queries.planning.nodes.physical.transform

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.transform.LimitOperator
import kotlin.math.min

/**
 * A [UnaryPhysicalOperatorNode] that represents the application of a LIMIT or SKIP clause on the result.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class LimitPhysicalOperatorNode(input: Physical? = null, val limit: Long, val skip: Long) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Fetch"
    }

    /** The name of this [LimitPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The output size of this [LimitPhysicalOperatorNode], which depends on skip and limit. */
    override val outputSize: Long
        get() = min((super.outputSize - this.skip), this.limit)

    /** The [Cost] of a [LimitPhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost(cpu = this.outputSize * Cost.COST_MEMORY_ACCESS)


    init {
        require(this.limit > 0) { "Limit must be greater than zero but isn't (limit = $limit)." }
        require(this.limit >= 0) { "Skip must be greater or equal to zero but isn't (limit = $skip)." }
    }

    /**
     * Creates and returns a copy of this [LimitPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [LimitPhysicalOperatorNode].
     */
    override fun copy() = LimitPhysicalOperatorNode(limit = this.limit, skip = this.skip)

    /**
     * Partitions this [LimitPhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<Physical> =
        this.input?.partition(p)?.map { LimitPhysicalOperatorNode(it, this.limit, this.skip) } ?: throw IllegalStateException("Cannot partition disconnected OperatorNode (node = $this)")

    /**
     * Converts this [LimitPhysicalOperatorNode] to a [LimitOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext) = LimitOperator(
        this.input?.toOperator(tx, ctx) ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)"),
        this.skip, this.limit
    )

    /** Generates and returns a [String] representation of this [LimitPhysicalOperatorNode]. */
    override fun toString() = "${this.groupId}:Limit[${this.skip},${this.limit}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is LimitPhysicalOperatorNode) return false

        if (limit != other.limit) return false
        if (skip != other.skip) return false

        return true
    }

    override fun hashCode(): Int {
        var result = limit.hashCode()
        result = 31 * result + skip.hashCode()
        return result
    }
}