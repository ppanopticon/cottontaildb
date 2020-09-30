package org.vitrivr.cottontail.database.queries.planning.nodes.logical

/**
 * An abstract [LogicalNodeExpression] implementation that has no input.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class NullaryLogicalNodeExpression : LogicalNodeExpression() {
    /** Input arity of [NullaryLogicalNodeExpression] is always zero. */
    final override val inputArity: Int = 0
}