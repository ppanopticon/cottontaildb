package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow

import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.basics.OperatorStatus
import org.vitrivr.cottontail.execution.operators.basics.PipelineBreaker
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.LongValue

/**
 * An [PipelineBreaker] used during query execution. It counts the number of rows it encounters
 * and returns the value as [Record].
 *
 * Only produces a single [Record].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class CountProjectionOperator(parent: Operator, context: ExecutionEngine.ExecutionContext) : PipelineBreaker(parent, context) {
    /** Column returned by [CountProjectionOperator]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(ColumnDef.withAttributes(parent.columns.first().name.entity()?.column("count()")
            ?: Name.ColumnName("count()"), "LONG"))


    override fun prepareOpen() { /* NoOp. */
    }

    override fun prepareClose() { /* NoOp. */
    }

    /**
     * Converts this [CountProjectionOperator] to a [Flow] and returns it.
     *
     * @param scope The [CoroutineScope] used for execution
     * @return [Flow] representing this [CountProjectionOperator]
     *
     * @throws IllegalStateException If this [Operator.status] is not [OperatorStatus.OPEN]
     */
    override fun toFlow(scope: CoroutineScope): Flow<Record> {
        check(this.status == OperatorStatus.OPEN) { "Cannot convert operator $this to flow because it is in state ${this.status}." }

        val parentFlow = this.parent.toFlow(scope)
        return flow {
            var counter = 0L
            parentFlow.collect {
                counter += 1
            }
            emit(StandaloneRecord(0L, this@CountProjectionOperator.columns, arrayOf(LongValue(counter))))
        }
    }
}