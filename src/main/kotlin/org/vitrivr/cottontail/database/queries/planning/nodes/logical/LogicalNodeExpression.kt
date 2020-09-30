package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression

/**
 * A logical [NodeExpression] in the Cottontail DB query execution plan.
 *
 * [LogicalNodeExpression]s are purely abstract and cannot be executed directly. They belong to the
 * first phase of the query optimization process, in which a plain input expression is transformed
 * into equivalent, logical expressions.
 *
 * @author Ralph Gasser
 * @version 1.0
 *
 * @see NodeExpression
 * @see PhysicalNodeExpression
 */
abstract class LogicalNodeExpression : NodeExpression() {
    /** [LogicalNodeExpression]s are never executable. */
    override val executable: Boolean = false

    /**
     * Creates and returns a copy of this [LogicalNodeExpression] without any children or parents.
     *
     * @return Copy of this [LogicalNodeExpression].
     */
    abstract override fun copy(): LogicalNodeExpression
}