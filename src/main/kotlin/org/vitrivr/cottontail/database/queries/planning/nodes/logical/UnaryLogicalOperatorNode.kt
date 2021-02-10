package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.queries.OperatorNode

/**
 * An abstract [LogicalOperatorNode] implementation that has a single [OperatorNode] as input.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class UnaryLogicalOperatorNode : LogicalOperatorNode() {
    /** Input arity of [UnaryLogicalOperatorNode] is always one. */
    final override val inputArity: Int = 1

    /** Reference to the input [LogicalOperatorNode]. */
    val input: OperatorNode?
        get() = this.inputs.getOrNull(0)
}