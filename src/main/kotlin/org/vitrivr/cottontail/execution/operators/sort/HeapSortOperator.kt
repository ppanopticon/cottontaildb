package org.vitrivr.cottontail.execution.operators.sort

import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [Operator.PipelineOperator] used during query execution. Performs sorting on the specified [ColumnDef]s and
 * returns the [Record] in sorted order. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
open class HeapSortOperator(parent: Operator, sortOn: Array<Pair<ColumnDef<*>, SortOrder>>, queueSize: Int) : AbstractSortOperator(parent, sortOn) {

    /** The internal [ObjectHeapPriorityQueue] used for sorting. */
    protected open val queue = ObjectHeapPriorityQueue(queueSize, this.comparator)

    /**
     * Converts this [HeapSortOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [HeapSortOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val parentFlow = this.parent.toFlow(context)
        return flow {
            parentFlow.onEach { this@HeapSortOperator.queue.enqueue(it) }.collect()
            while (!this@HeapSortOperator.queue.isEmpty) {
                emit(this@HeapSortOperator.queue.dequeue())
            }
        }
    }
}