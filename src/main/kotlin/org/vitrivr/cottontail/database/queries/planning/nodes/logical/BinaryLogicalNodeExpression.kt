package org.vitrivr.cottontail.database.queries.planning.nodes.logical

/**
 * An abstract [BinaryLogicalNodeExpression] implementation that has exactly two [NodeExpression]s as input.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class BinaryLogicalNodeExpression : LogicalNodeExpression() {
    /** Input arity of [UnaryLogicalNodeExpression] is always one. */
    final override val inputArity: Int = 2

    /** Reference to the input [LogicalNodeExpression]. */
    val i1: LogicalNodeExpression
        get() {
            val input = this.inputs.getOrNull(0)
            if (input is LogicalNodeExpression) {
                return input
            } else {
                throw Exception()
            }
        }

    /** Reference to the input [LogicalNodeExpression]. */
    val i2: LogicalNodeExpression
        get() {
            val input = this.inputs.getOrNull(1)
            if (input is LogicalNodeExpression) {
                return input
            } else {
                throw Exception()
            }
        }
}