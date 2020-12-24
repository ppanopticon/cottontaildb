package org.vitrivr.cottontail.model.exceptions

import org.vitrivr.cottontail.execution.operators.basics.Operator

/**
 * [Exception] thrown during query execution.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
open class ExecutionException(message: String) : DatabaseException(message) {
    /**
     * This exception gets thrown whenever an [Operator] throws an [Exception] during execution.
     */
    class OperatorExecutionException(val operator: Operator, message: String) : ExecutionException("Execution error in operator $operator: $message")
}