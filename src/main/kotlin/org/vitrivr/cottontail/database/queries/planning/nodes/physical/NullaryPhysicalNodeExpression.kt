package org.vitrivr.cottontail.database.queries.planning.nodes.physical

/**
 * An abstract [PhysicalNodeExpression] implementation that has no input node.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class NullaryPhysicalNodeExpression : PhysicalNodeExpression() {
    /** The arity of the [NullaryPhysicalNodeExpression] is always on. */
    override val inputArity = 0
}