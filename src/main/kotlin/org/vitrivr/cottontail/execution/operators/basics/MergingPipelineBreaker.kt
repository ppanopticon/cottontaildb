package org.vitrivr.cottontail.execution.operators.basics

import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.model.recordset.Recordset

/**
 * An [Operator] that can be pipelined and has multiple, incoming parent [Operator]s that must
 * be materialized before processing can continue.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
abstract class MergingPipelineBreaker(val parents: List<Operator>, context: ExecutionEngine.ExecutionContext) : Operator(context) {
    /** Implementation of [Operator.open] */
    override fun open() {
        check(this.status == OperatorStatus.CREATED) { "Cannot open operator that is in state ${this.status}." }

        /* Call parents. */
        this.parents.forEach { it.open() }

        /* Execute preparation. */
        this.prepareOpen()

        /* Update status. */
        this.status = OperatorStatus.OPEN
    }

    /** Implementation of [Operator.close] */
    override fun close() {
        check(this.status == OperatorStatus.OPEN) { "Cannot close operator that is state ${this.status}." }

        /* Call parents. */
        this.parents.forEach { it.close() }

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
    abstract fun prepareOpen()

    /**
     * This method can be used to make necessary preparations prior to closing the operation.
     * Called by [Operator.close]. Nulls the cached [Recordset]
     *
     * Can be overridden by an implementing class.
     */
    abstract fun prepareClose()
}