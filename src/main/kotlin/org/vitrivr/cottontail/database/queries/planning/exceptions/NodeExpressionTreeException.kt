package org.vitrivr.cottontail.database.queries.planning.exceptions

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression

/**
 * Type of [Exception]s that are thrown while processing a [NodeExpression] or a [NodeExpression] tree.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
sealed class NodeExpressionTreeException(val exp: NodeExpression, message: String): Exception (message) {
    /**
     * Thrown when a [NodeExpression] tree is incomplete, i.e. [NodeExpression]s are missing, while trying process it.
     *
     * @param exp The [NodeExpression] that caused the exception.
     * @param message Explanation of the problem.
     */
    class IncompleteNodeExpressionTreeException(exp: NodeExpression, message: String) : NodeExpressionTreeException(exp, "NodeExpression $exp seems incomplete: $message")

    /**
     * Thrown when a [NodeExpression] cannot be materialized due to constraints of incoming or outgoing [NodeExpression]s.
     *
     * @param exp The [NodeExpression] that caused the exception.
     * @param message Explanation of the problem.
     */
    class InsatisfiableNodeExpressionException(exp: NodeExpression, message: String) : NodeExpressionTreeException(exp, "NodeExpression $exp seems incomplete: $message")
}

