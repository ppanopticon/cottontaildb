package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.exceptions.OperatorSetupException
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.ExecutionException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An [Operator.PipelineOperator] used during query execution. It calculates the MEAN of all values
 * it has encountered and returns it as a [Record].
 *
 * Only produces a single [Record] and converts the respective column to a [DoubleColumnType].
 * Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class MeanProjectionOperator(parent: Operator, fields: List<Pair<ColumnDef<*>, Name.ColumnName?>>) :
    Operator.PipelineOperator(parent) {

    /** [MaxProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /** Columns produced by [MeanProjectionOperator]. */
    override val columns: Array<ColumnDef<*>> = fields.map {
        if (!it.first.type.numeric) {
            throw OperatorSetupException(
                this,
                "The provided column ${it.first} cannot be used for a ${Projection.MEAN} projection. It either doesn't exist or has the wrong type."
            )
        }
        val alias = it.second
        if (alias != null) {
            ColumnDef(alias, Type.Double)
        } else {
            val columnNameStr = "${Projection.MEAN.label()}_${it.first.name.simple})"
            val columnName =
                it.first.name.entity()?.column(columnNameStr) ?: Name.ColumnName(columnNameStr)
            ColumnDef(columnName, Type.Double)
        }
    }.toTypedArray()

    /** Parent [ColumnDef] to access and aggregate. */
    private val parentColumns = fields.map { it.first }

    /**
     * Converts this [MeanProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [MeanProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val parentFlow = this.parent.toFlow(context)
        return flow {
            /* Prepare holder of type double. */
            val count = this@MeanProjectionOperator.parentColumns.map { 0L }.toTypedArray()
            val sum = this@MeanProjectionOperator.parentColumns.map { 0.0 }.toTypedArray()
            parentFlow.collect {
                this@MeanProjectionOperator.parentColumns.forEachIndexed { i, c ->
                    val value = it[c]
                    if (value != null) {
                        count[i] += 1L
                        sum[i] += when (value) {
                            is ByteValue -> value.value.toDouble()
                            is ShortValue -> value.value.toDouble()
                            is IntValue -> value.value.toDouble()
                            is LongValue -> value.value.toDouble()
                            is FloatValue -> value.value.toDouble()
                            is DoubleValue -> value.value
                            null -> 0.0
                            else -> throw ExecutionException.OperatorExecutionException(
                                this@MeanProjectionOperator,
                                "The provided column $c cannot be used for a ${Projection.MEAN} projection."
                            )
                        }
                    }
                }
            }

            /** Emit record. */
            val results = Array<Value?>(sum.size) { DoubleValue(sum[it] / count[it]) }
            emit(StandaloneRecord(0L, this@MeanProjectionOperator.columns, results))
        }
    }
}