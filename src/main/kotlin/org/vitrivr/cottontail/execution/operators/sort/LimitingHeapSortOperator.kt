package org.vitrivr.cottontail.execution.operators.sort

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.basics.drop
import org.vitrivr.cottontail.math.knn.selection.HeapSelection
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [Operator.PipelineOperator] used during query execution. Performs sorting on the specified
 * [ColumnDef]s and returns the [Record] in sorted order. Limits the number of [Record]s produced.
 *
 * Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LimitingHeapSortOperator(parent: Operator, sortOn: Array<Pair<ColumnDef<*>, SortOrder>>, private val limit: Long, private val skip: Long) : AbstractSortOperator(parent, sortOn) {

    /** The [HeapSortOperator] retains the [ColumnDef] of the input. */
    override val columns: Array<ColumnDef<*>> = this.parent.columns

    /** The [HeapSortOperator] is always a pipeline breaker. */
    override val breaker: Boolean = true

    /**
     * Converts this [LimitingHeapSortOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [LimitingHeapSortOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val parentFlow = if (this.skip > 0) {
            this.parent.toFlow(context).drop(this.skip)
        } else {
            this.parent.toFlow(context)
        }
        return flow {
            val selection = HeapSelection(this@LimitingHeapSortOperator.limit, this@LimitingHeapSortOperator.comparator)
            parentFlow.collect { selection.offer(it.copy()) }
            for (i in 0 until selection.size) {
                emit(selection[i])
            }
        }
    }
}