package org.vitrivr.cottontail.database.queries.planning.nodes.physical

import org.vitrivr.cottontail.database.queries.OperatorNode

/**
 * An abstract [PhysicalOperatorNode] implementation that has a single [OperatorNode] as input.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class UnaryPhysicalOperatorNode : PhysicalOperatorNode() {
    /** The arity of the [UnaryPhysicalOperatorNode] is always on. */
    override val inputArity = 1

    /** [UnaryPhysicalOperatorNode] can be partitioned, if its parent can be partitioned. */
    override val canBePartitioned: Boolean
        get() = this.input.canBePartitioned

    /**
     * Tries to create [p] partitions of this [UnaryPhysicalOperatorNode], which is possible
     * as long as all upstream [PhysicalOperatorNode]s can be partitioned.
     *
     * @param p The desired number of partitions.
     * @return Array of [PhysicalOperatorNode]s.
     *
     * @throws IllegalStateException If this [PhysicalOperatorNode] cannot be partitioned.
     */
    override fun partition(p: Int): List<PhysicalOperatorNode> = this.input.partition(p).map {
        val copy = this.copy()
        copy.addInput(it)
        copy
    }

    /** Reference to the input [PhysicalOperatorNode]. */
    val input: PhysicalOperatorNode
        get() {
            val input = this.inputs.getOrNull(0)
            if (input is PhysicalOperatorNode) {
                return input
            } else {
                throw Exception()
            }
        }
}