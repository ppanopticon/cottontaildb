package org.vitrivr.cottontail.execution.operators.transform

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flattenMerge
import kotlinx.coroutines.flow.flowOf
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Record

/**
 * A [MergeOperator] merges the results of multiple incoming operators into a single [Flow].
 *
 * The incoming [Operator]s are executed in parallel, hence order of the [Record]s in the
 * outgoing [Flow] may be arbitrary.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class MergeOperator(parents: List<Operator>) : Operator.MergingPipelineOperator(parents) {

    /** The columns produced by this [MergeOperator]. */
    override val columns: Array<ColumnDef<*>> = this.parents.first().columns

    /** [MergeOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [MergeOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [MergeOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        /* Obtain parent flows amd compose new flow. */
        val parentFlows = flowOf(*this.parents.map { it.toFlow(context) }.toTypedArray())
        return parentFlows.flattenMerge(this.parents.size)
    }
}