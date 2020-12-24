package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.exceptions.OperatorSetupException
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.ExecutionException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.*

/**
 * An [Operator.PipelineOperator] used during query execution. It calculates the MEAN of all values
 * it has encountered  and returns it as a [Record].
 *
 * Only produces a single [Record]. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.1.1
 */
class MeanProjectionOperator(parent: Operator, val name: Name.ColumnName, val alias: Name.ColumnName? = null) : Operator.PipelineOperator(parent) {

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

    /** [MaxProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /**
     * Converts this [MeanProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [MeanProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val parentFlow = this.parent.toFlow(context)
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
                    else -> throw ExecutionException.OperatorExecutionException(this@MeanProjectionOperator, "The provided column $name cannot be used for a MEAN projection.")
                }
            }
            emit(StandaloneRecord(0L, this@MeanProjectionOperator.columns, arrayOf(DoubleValue(sum / count))))
        }
    }
}