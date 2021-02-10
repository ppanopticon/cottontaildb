package org.vitrivr.cottontail.database.queries.planning.nodes.physical

/**
 * An abstract [PhysicalOperatorNode] implementation that has no input node.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class NullaryPhysicalOperatorNode : PhysicalOperatorNode() {
    /** The arity of the [NullaryPhysicalOperatorNode] is always on. */
    override val inputArity = 0
}