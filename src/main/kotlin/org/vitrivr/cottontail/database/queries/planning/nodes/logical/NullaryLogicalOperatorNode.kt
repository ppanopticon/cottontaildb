package org.vitrivr.cottontail.database.queries.planning.nodes.logical

/**
 * An abstract [LogicalOperatorNode] implementation that has no input.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class NullaryLogicalOperatorNode : LogicalOperatorNode() {
    /** Input arity of [NullaryLogicalOperatorNode] is always zero. */
    final override val inputArity: Int = 0
}