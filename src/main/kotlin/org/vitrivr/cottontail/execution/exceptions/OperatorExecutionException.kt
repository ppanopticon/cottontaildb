package org.vitrivr.cottontail.execution.exceptions

import org.vitrivr.cottontail.execution.operators.basics.Operator

/**
 * This exception gets thrown whenever an [Operator] throws an [Exception] during execution.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class OperatorExecutionException(val operator: Operator, message: String) : ExecutionException("Execution error in operator $operator: $message")