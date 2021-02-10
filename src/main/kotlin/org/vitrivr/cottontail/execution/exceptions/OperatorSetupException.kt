package org.vitrivr.cottontail.execution.exceptions

import org.vitrivr.cottontail.execution.operators.basics.Operator

/**
 * This exception gets thrown by the execution engine whenever [Operator] throws an exception
 * during  setup either when instantiated or in the preparation phase.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class OperatorSetupException(val operator: Operator, override val message: String) : Exception("Setup error in operator $operator: $message")