package org.vitrivr.cottontail.execution.operators.sinks

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Record

/**
 * A [Operator.SinkOperator] used during query execution. Extracts the produced [Record]s into a specified list.
 *
 * @param parent [Operator] that produces the results.
 * @param into The [MutableList] to write [Record] into.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class RecordCollectorSink(parent: Operator, val into: MutableList<Record>) : Operator.SinkOperator(parent) {
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val parentFlow = this.parent.toFlow(context)
        return flow {
            this@RecordCollectorSink.into.clear()
            parentFlow.onEach { this@RecordCollectorSink.into.add(it) }.collect()
        }
    }
}