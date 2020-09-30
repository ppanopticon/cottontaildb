package org.vitrivr.cottontail.execution.operators.basics

/**
 * Status of an [Operator]. Determines whether [Operator] can be converted to a [Flow] and executed.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
enum class OperatorStatus {
    CREATED, OPEN, CLOSED
}