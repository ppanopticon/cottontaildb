package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.queries.OperatorNode

/**
 * A logical [OperatorNode] in the Cottontail DB query execution plan.
 *
 * [LogicalOperatorNode]s are purely abstract and cannot be executed directly. They belong to the
 * first phase of the query optimization process, in which the canonical input [LogicalOperatorNode]
 * are transformed into equivalent representations of [LogicalOperatorNode]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 *
 * @see OperatorNode
 */
abstract class LogicalOperatorNode : OperatorNode() {
    /** [LogicalOperatorNode]s are never executable. */
    override val executable: Boolean = false

    /**
     * Creates and returns a copy of this [LogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [LogicalOperatorNode].
     */
    abstract override fun copy(): LogicalOperatorNode

    /**
     * Calculates and returns the digest for this [LogicalOperatorNode].
     *
     * @return Digest for this [LogicalOperatorNode]
     */
    override fun digest(): Long = this.javaClass.hashCode().toLong()
}