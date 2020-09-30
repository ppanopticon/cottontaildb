package org.vitrivr.cottontail.database.queries.planning.nodes.physical

/**
 * An abstract [PhysicalNodeExpression] implementation that has a single [NodeExpression] as input.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class UnaryPhysicalNodeExpression : PhysicalNodeExpression() {
    /** The arity of the [UnaryPhysicalNodeExpression] is always on. */
    override val inputArity = 1

    /** Reference to the input [PhysicalNodeExpression]. */
    val input: PhysicalNodeExpression
        get() {
            val input = this.inputs.getOrNull(0)
            if (input is PhysicalNodeExpression) {
                return input
            } else {
                throw Exception()
            }
        }
}