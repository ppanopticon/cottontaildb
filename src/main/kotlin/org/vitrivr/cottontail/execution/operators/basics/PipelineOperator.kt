package org.vitrivr.cottontail.execution.operators.basics

import org.vitrivr.cottontail.execution.ExecutionEngine

/**
 * An [Operator] that can be pipelined, i.e., has a parent [Operator] and no materialization of
 * intermediate results is required.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
abstract class PipelineOperator(val parent: Operator, context: ExecutionEngine.ExecutionContext) : Operator(context) {
    /** Implementation of [Operator.open] */
    final override fun open() {
        check(this.status == OperatorStatus.CREATED) { "Cannot open operator that is in state ${this.status}." }

        /* Call parent.open(). */
        val p = this.parent
        p.open()

        /* Execute preparation. */
        this.prepareOpen()

        /* Update status. */
        this.status = OperatorStatus.OPEN
    }

    /** Implementation of [Operator.close] */
    final override fun close() {
        check(this.status == OperatorStatus.OPEN) { "Cannot close operator that is state ${this.status}." }

        /* Propagate to parent. */
        this.parent.close()

        /* Execute preparation. */
        this.prepareClose()

        /* Update status. */
        this.status = OperatorStatus.CLOSED
    }

    /**
     * This method can be used to make necessary preparations, e.g., acquire relevant locks,
     * pre-fetch data etc. prior to query execution. Called by [Operator.open].
     *
     * Can be overridden by an implementing class.
     */
    protected abstract fun prepareOpen()

    /**
     * This method can be used to make necessary preparations prior to closing the operation.
     * Called by [Operator.close].
     *
     * Can be overridden by an implementing class.
     */
    protected abstract fun prepareClose()
}