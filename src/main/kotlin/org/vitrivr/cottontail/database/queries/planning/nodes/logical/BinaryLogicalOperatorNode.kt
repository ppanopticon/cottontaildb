package org.vitrivr.cottontail.database.queries.planning.nodes.logical

/**
 * An abstract [BinaryLogicalOperatorNode] implementation that has exactly two [NodeExpression]s as input.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class BinaryLogicalOperatorNode : LogicalOperatorNode() {
    /** Input arity of [UnaryLogicalOperatorNode] is always one. */
    final override val inputArity: Int = 2

    /** Reference to the input [LogicalOperatorNode]. */
    val i1: LogicalOperatorNode
        get() {
            val input = this.inputs.getOrNull(0)
            if (input is LogicalOperatorNode) {
                return input
            } else {
                throw Exception()
            }
        }

    /** Reference to the input [LogicalOperatorNode]. */
    val i2: LogicalOperatorNode
        get() {
            val input = this.inputs.getOrNull(1)
            if (input is LogicalOperatorNode) {
                return input
            } else {
                throw Exception()
            }
        }
}