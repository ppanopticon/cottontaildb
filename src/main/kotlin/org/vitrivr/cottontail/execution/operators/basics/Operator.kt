package org.vitrivr.cottontail.execution.operators.basics

import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.execution.TransactionContext

import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [Operator] used during query execution and processing.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed class Operator {

    /** The list of [ColumnDef]s produced by this [Operator]. */
    abstract val columns: Array<ColumnDef<*>>

    /**
     * Converts this [Operator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution.
     * @return [Flow]
     */
    abstract fun toFlow(context: TransactionContext): Flow<Record>


    /**
     * An [Operator] that can be pipelined, i.e., has a parent [Operator] and no materialization of
     * intermediate results is required.
     *
     * @author Ralph Gasser
     * @version 1.1.1
     */
    abstract class PipelineOperator(val parent: Operator) : Operator() {
        /** Flag indicating whether this [PipelineOperator] acts as a pipeline breaker. */
        abstract val breaker: Boolean
    }

    /**
     * An [Operator] that can be pipelined and has multiple, incoming parent [Operator]s.
     *
     * @author Ralph Gasser
     * @version 1.1.1
     */
    abstract class MergingPipelineOperator(val parents: List<Operator>) : Operator() {
        /** Flag indicating whether this [MergingPipelineOperator] acts as a pipeline breaker. */
        abstract val breaker: Boolean
    }

    /**
     * An [Operator] that acts as a sink, i.e., processes and consumes [Record]s.
     *
     * @author Ralph Gasser
     * @version 1.1.2
     */
    abstract class SinkOperator(val parent: Operator) : Operator() {
        final override val columns: Array<ColumnDef<*>> = emptyArray()
    }

    /**
     * An [Operator] that acts as a source, i.e., thus has no parent [Operator].
     *
     * @author Ralph Gasser
     * @version 1.1.1
     */
    abstract class SourceOperator : Operator()
}