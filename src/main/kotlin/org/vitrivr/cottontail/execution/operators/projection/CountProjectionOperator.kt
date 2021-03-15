package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.LongValue

/**
 * An [Operator.PipelineOperator] used during query execution. It counts the number of rows it
 * encounters and returns the value as [Record].
 *
 * Only produces a single [Record]. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class CountProjectionOperator(parent: Operator) : Operator.PipelineOperator(parent) {
    /** Column returned by [CountProjectionOperator]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(
            name = parent.columns.first().name.entity()?.column(Projection.COUNT.label())
                ?: Name.ColumnName(Projection.COUNT.label()),
            type = Type.Long
        )
    )

    /** [CountProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /**
     * Converts this [CountProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [CountProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val parentFlow = this.parent.toFlow(context)
        return flow {
            emit(StandaloneRecord(0L, this@CountProjectionOperator.columns[0], LongValue(parentFlow.count())))
        }
    }
}