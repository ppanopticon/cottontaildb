package org.vitrivr.cottontail.database.queries.planning.exceptions

import org.vitrivr.cottontail.database.queries.OperatorNode

/**
 * Type of [Exception]s that are thrown while processing a [OperatorNode] or a [OperatorNode] tree.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
sealed class NodeExpressionTreeException(val exp: OperatorNode, message: String) :
    Exception(message) {
    /**
     * Thrown when a [OperatorNode] tree is incomplete, i.e. [OperatorNode]s are missing, while trying process it.
     *
     * @param exp The [OperatorNode] that caused the exception.
     * @param message Explanation of the problem.
     */
    class IncompleteNodeExpressionTreeException(exp: OperatorNode, message: String) :
        NodeExpressionTreeException(exp, "NodeExpression $exp seems incomplete: $message")

    /**
     * Thrown when a [OperatorNode] cannot be materialized due to constraints of incoming or outgoing [OperatorNode]s.
     *
     * @param exp The [OperatorNode] that caused the exception.
     * @param message Explanation of the problem.
     */
    class InsatisfiableNodeExpressionException(exp: OperatorNode, message: String) :
        NodeExpressionTreeException(exp, "NodeExpression $exp seems incomplete: $message")
}

