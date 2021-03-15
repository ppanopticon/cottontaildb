package org.vitrivr.cottontail.execution.operators.transform

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.*

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.sort.HeapSortOperator
import org.vitrivr.cottontail.execution.operators.sort.RecordComparator
import org.vitrivr.cottontail.math.knn.selection.HeapSelection
import org.vitrivr.cottontail.model.basics.Record

/**
 * A [MergeLimitingHeapSortOperator] merges the results of multiple incoming operators into a single [Flow],
 * orders them by a specified [ColumnDef] and limits the number of output [Record]s.
 *
 * This is often used in parallelized nearest neighbour queries.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class MergeLimitingHeapSortOperator(parents: List<Operator>, sortOn: Array<Pair<ColumnDef<*>, SortOrder>>, limit: Long) : Operator.MergingPipelineOperator(parents) {

    /** The columns produced by this [MergeOperator]. */
    override val columns: Array<ColumnDef<*>> = this.parents.first().columns

    /** The [HeapSortOperator] is always a pipeline breaker. */
    override val breaker: Boolean = true

    /** The [Comparator] used for sorting. */
    private val comparator: Comparator<Record> = when {
        sortOn.size == 1 && sortOn.first().first.nullable -> RecordComparator.SingleNullColumnComparator(sortOn.first().first, sortOn.first().second)
        sortOn.size == 1 && !sortOn.first().first.nullable -> RecordComparator.SingleNonNullColumnComparator(sortOn.first().first, sortOn.first().second)
        sortOn.size > 1 && !sortOn.any { it.first.nullable } -> RecordComparator.MultiNullColumnComparator(sortOn)
        else -> RecordComparator.MultiNonNullColumnComparator(sortOn)
    }

    /** The [HeapSelection] used for sorting. */
    private val selection = HeapSelection(limit, this.comparator)

    /**
     * Converts this [MergeOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [MergeOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val parentFlows = this.parents.map { it.toFlow(context) }
        return flow {
            val ctx = currentCoroutineContext()
            /* Execute incoming flows and wait for completion. */
            parentFlows.map { flow ->
                flow.onEach { record ->
                    this@MergeLimitingHeapSortOperator.selection.offer(record)
                }.launchIn(CoroutineScope(ctx))
            }.forEach {
                it.join()
            }

            /* Emit sorted and limited values. */
            for (i in 0 until this@MergeLimitingHeapSortOperator.selection.size) {
                emit(this@MergeLimitingHeapSortOperator.selection[i])
            }
        }
    }
}