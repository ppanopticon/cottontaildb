package org.vitrivr.cottontail.execution.operators.sort

import kotlinx.coroutines.flow.*
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
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
class MergeLimitingHeapSortOperator(parents: List<Operator>, sortOn: Array<Pair<ColumnDef<*>, SortOrder>>, val limit: Long) : Operator.MergingPipelineOperator(parents) {

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

    /**
     * Converts this [MergeLimitingHeapSortOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [MergeLimitingHeapSortOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        return flow {
            val selection = HeapSelection(this@MergeLimitingHeapSortOperator.limit, this@MergeLimitingHeapSortOperator.comparator)
            val parentFlows = this@MergeLimitingHeapSortOperator.parents.map {
                it.toFlow(context).onEach { record ->
                    selection.offer(record.copy())
                }
            }.toTypedArray()
            flowOf(*parentFlows).flattenMerge(this@MergeLimitingHeapSortOperator.parents.size).collect()

            /* Emit sorted and limited values. */
            for (i in 0 until selection.size) {
                emit(selection[i])
            }
        }
    }
}