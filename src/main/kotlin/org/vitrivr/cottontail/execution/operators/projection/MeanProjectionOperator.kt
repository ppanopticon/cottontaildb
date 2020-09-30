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
 * An [PipelineBreaker] used during query execution. It calculates the MEAN of all values
 * it has encountered  and returns it as a [Record].
 *
 * Only produces a single [Record].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class MeanProjectionOperator(parent: Operator, context: ExecutionEngine.ExecutionContext, val name: Name.ColumnName, val alias: Name.ColumnName? = null) : PipelineBreaker(parent, context) {

    /** The [ColumnDef] of the incoming [Operator] that is being used for calculation. */
    private val parentColumn: ColumnDef<*> = this.parent.columns.firstOrNull { it.name == name && it.type.numeric }
            ?: throw OperatorSetupException(this, "The provided column $name cannot be used for a MEAN projection. It either doesn't exist or has the wrong type.")

    /** Columns produced by [MeanProjectionOperator]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
            ColumnDef.withAttributes(
                    this.alias
                            ?: (parentColumn.name.entity()?.column("mean(${this.parentColumn.name.simple})")
                                    ?: Name.ColumnName("mean(${this.parentColumn.name.simple}")),
                    "DOUBLE"
            )
    )

    override fun prepareOpen() { /*NoOp */
    }

    override fun prepareClose() { /*NoOp */
    }

    /**
     * Converts this [MeanProjectionOperator] to a [Flow] and returns it.
     *
     * @param scope The [CoroutineScope] used for execution
     * @return [Flow] representing this [MeanProjectionOperator]
     * @throws IllegalStateException If this [Operator.status] is not [OperatorStatus.OPEN]
     */
    override fun toFlow(scope: CoroutineScope): Flow<Record> {
        check(this.status == OperatorStatus.OPEN) { "Cannot convert operator $this to flow because it is in state ${this.status}." }

        val parentFlow = this.parent.toFlow(scope)
        return flow {
            var sum = 0.0
            var count = 0L
            parentFlow.collect {
                val value = it[this@MeanProjectionOperator.parentColumn]
                count++
                sum += when (value) {
                    is ByteValue -> value.value.toDouble()
                    is ShortValue -> value.value.toDouble()
                    is IntValue -> value.value.toDouble()
                    is LongValue -> value.value.toDouble()
                    is FloatValue -> value.value.toDouble()
                    is DoubleValue -> value.value
                    else -> throw OperatorExecutionException(this@MeanProjectionOperator, "The provided column $name cannot be used for a MEAN projection.")
                }
            }
            emit(StandaloneRecord(0L, this@MeanProjectionOperator.columns, arrayOf(DoubleValue(sum / count))))
        }
    }
}