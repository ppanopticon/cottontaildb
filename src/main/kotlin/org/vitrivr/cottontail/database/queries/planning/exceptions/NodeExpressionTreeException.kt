package org.vitrivr.cottontail.database.queries.planning.exceptions

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression


sealed class NodeExpressionTreeException(val exp: NodeExpression, message: String): Exception (message) {


    /**
     *
     */
    class IncompleteNodeExpressionTreeException(exp: NodeExpression, message: String): NodeExpressionTreeException(exp, "NodeExpression $exp seems incomplete: $message")
}

