package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.AbortFlowException
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.basics.OperatorStatus
import org.vitrivr.cottontail.execution.operators.basics.PipelineBreaker
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.BooleanValue

/**
 * An [PipelineBreaker] used during query execution. It returns a single [Record] containing
 * either true or false depending on whether there are incoming [Record]s.
 *
 * Only produces a single [Record].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class ExistsProjectionOperator(parent: Operator, context: ExecutionEngine.ExecutionContext) : PipelineBreaker(parent, context) {

    /** Column returned by [ExistsProjectionOperator]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(ColumnDef.withAttributes(parent.columns.first().name.entity()?.column("exists()")
            ?: Name.ColumnName("exists()"), "BOOLEAN"))


    override fun prepareOpen() { /*NoOp. */
    }

    override fun prepareClose() { /* NoOp. */
    }

    /**
     * Converts this [ExistsProjectionOperator] to a [Flow] and returns it.
     *
     * @param scope The [CoroutineScope] used for execution
     * @return [Flow] representing this [ExistsProjectionOperator]
     * @throws IllegalStateException If this [Operator.status] is not [OperatorStatus.OPEN]
     */
    override fun toFlow(scope: CoroutineScope): Flow<Record> {
        check(this.status == OperatorStatus.OPEN) { "Cannot convert operator $this to flow because it is in state ${this.status}." }

        val parentFlow = this.parent.toFlow(scope)
        return flow {
            var exists = false
            try {
                parentFlow.collect {
                    exists = true
                    throw AbortFlowException(this)
                }
            } catch (e: AbortFlowException) {
                e.checkOwnership(this)
            }
            emit(StandaloneRecord(0L, this@ExistsProjectionOperator.columns, arrayOf(BooleanValue(exists))))
        }
    }
}