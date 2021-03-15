package org.vitrivr.cottontail.execution.operators.sinks

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.exceptions.OperatorSetupException
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.values.types.Value
import java.util.*

/**
 * A [Operator.SinkOperator] used during query execution. Extracts the produced [Value]s into a specified list.
 *
 * @param parent [Operator] that produces the results.
 * @param column The [ColumnDef] of the column to extract.
 * @param into The [MutableList] to write values into.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ValueCollectorSink(parent: Operator, val column: ColumnDef<*>, val into: MutableList<Value?>) : Operator.SinkOperator(parent) {

    init {
        if (!parent.columns.contains(column)) {
            throw OperatorSetupException(this, "The specified column ${this.column} is not produced by the parent.")
        }
    }

    override fun toFlow(context: TransactionContext): Flow<Record> {
        val parentFlow = this.parent.toFlow(context)
        return flow {
            this@ValueCollectorSink.into.clear()
            parentFlow.onEach {
                val value = it[this@ValueCollectorSink.column]
                this@ValueCollectorSink.into.add(value)
            }.collect()
        }
    }
}