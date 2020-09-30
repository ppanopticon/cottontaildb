package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.exceptions.OperatorExecutionException
import org.vitrivr.cottontail.execution.exceptions.OperatorSetupException
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.basics.OperatorStatus
import org.vitrivr.cottontail.execution.operators.basics.PipelineBreaker
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.*

/**
 * An [Operator.PipelineBreaker] used during query execution. It calculates the SUM of all values it
 * has encountered and returns it as a [Record]. Only produces a single [Record].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class SumProjectionOperator(parent: Operator, context: ExecutionEngine.ExecutionContext, val name: Name.ColumnName, val alias: Name.ColumnName? = null) : PipelineBreaker(parent, context) {
    /** The [ColumnDef] of the incoming [Operator] that is being used for calculation. */
    private val parentColumn: ColumnDef<*> = this.parent.columns.firstOrNull { it.name == name && it.type.numeric }
            ?: throw OperatorSetupException(this, "The provided column $name cannot be used for a SUM projection. It either doesn't exist or has the wrong type.")

    /** Columns produced by [MeanProjectionOperator]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
            ColumnDef.withAttributes(
                    this.alias
                            ?: (parentColumn.name.entity()?.column("sum(${this.parentColumn.name.simple})")
                                    ?: Name.ColumnName("sum(${this.parentColumn.name.simple}")),
                    "DOUBLE"
            )
    )

    override fun prepareOpen() { /*NoOp */
    }

    override fun prepareClose() { /*NoOp */
    }

    /**
     * Converts this [SumProjectionOperator] to a [Flow] and returns it.
     *
     * @param scope The [CoroutineScope] used for execution
     * @return [Flow] representing this [SumProjectionOperator]
     * @throws IllegalStateException If this [Operator.status] is not [OperatorStatus.OPEN]
     */
    override fun toFlow(scope: CoroutineScope): Flow<Record> {
        check(this.status == OperatorStatus.OPEN) { "Cannot convert operator $this to flow because it is in state ${this.status}." }

        val parentFlow = this.parent.toFlow(scope)
        return flow {
            var sum = 0.0
            parentFlow.collect {
                val value = it[this@SumProjectionOperator.parentColumn]
                sum += when (value) {
                    is ByteValue -> value.value.toDouble()
                    is ShortValue -> value.value.toDouble()
                    is IntValue -> value.value.toDouble()
                    is LongValue -> value.value.toDouble()
                    is FloatValue -> value.value.toDouble()
                    is DoubleValue -> value.value
                    else -> throw OperatorExecutionException(this@SumProjectionOperator, "The provided column $parentColumn cannot be used for a SUM projection.")
                }
            }
            emit(StandaloneRecord(0L, this@SumProjectionOperator.columns, arrayOf(DoubleValue(sum))))
        }
    }
}