package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression

/**
 * An abstract [LogicalNodeExpression] implementation that has a single [NodeExpression] as input.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class UnaryLogicalNodeExpression : LogicalNodeExpression() {
    /** Input arity of [UnaryLogicalNodeExpression] is always one. */
    final override val inputArity: Int = 1

    /** Reference to the input [LogicalNodeExpression]. */
    val input: NodeExpression?
        get() =  this.inputs.getOrNull(0)
}