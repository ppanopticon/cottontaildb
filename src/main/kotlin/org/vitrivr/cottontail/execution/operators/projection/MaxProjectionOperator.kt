package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import org.vitrivr.cottontail.database.column.*
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
import kotlin.math.max

/**
 * A [Operator.PipelineOperator] used during query execution. It tracks the MAX (maximum) it has
 * encountered so far for each column and returns it as a [Record].
 *
 * The [MaxProjectionOperator] retains the [Type] of the incoming entries! Only produces a
 * single [Record]. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class MaxProjectionOperator(
    parent: Operator,
    fields: List<Pair<Name.ColumnName, Name.ColumnName?>>
) : Operator.PipelineOperator(parent) {

    /** [MaxProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /** Columns produced by [MaxProjectionOperator]. */
    override val columns: Array<ColumnDef<*>> = this.parent.columns.mapNotNull { c ->
        val match = fields.find { f -> f.first.matches(c.name) }
        if (match != null) {
            if (!c.type.numeric) throw OperatorSetupException(
                this,
                "The provided column $match cannot be used for a ${Projection.MAX} projection because it has the wrong type."
            )
            val alias = match.second
            if (alias != null) {
                c.copy(name = alias)
            } else {
                val columnNameStr = "${Projection.MAX.label()}_${c.name.simple})"
                val columnName =
                    c.name.entity()?.column(columnNameStr) ?: Name.ColumnName(columnNameStr)
                c.copy(name = columnName)
            }
        } else {
            null
        }
    }.toTypedArray()

    /** Parent [ColumnDef] to access and aggregate. */
    private val parentColumns = this.parent.columns.filter { c ->
        fields.any { f -> f.first.matches(c.name) }
    }

    /**
     * Converts this [CountProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [CountProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val parentFlow = this.parent.toFlow(context)
        return flow {
            /* Prepare holder of type double, which can hold all types of values and collect incoming flow */
            val max = this@MaxProjectionOperator.parentColumns.map { Double.MIN_VALUE }.toTypedArray()
            parentFlow.onEach {
                this@MaxProjectionOperator.parentColumns.forEachIndexed { i, c ->
                    max[i] = when (val value = it[c]) {
                        is ByteValue -> max(max[i], value.value.toDouble())
                        is ShortValue -> max(max[i], value.value.toDouble())
                        is IntValue -> max(max[i], value.value.toDouble())
                        is LongValue -> max(max[i], value.value.toDouble())
                        is FloatValue -> max(max[i], value.value.toDouble())
                        is DoubleValue -> max(max[i], value.value)
                        null -> max[i]
                        else -> throw ExecutionException.OperatorExecutionException(this@MaxProjectionOperator, "The provided column $c cannot be used for a MAX projection. ")
                    }
                }
            }.collect()

            /* Convert to original value type. */
            val results = Array<Value?>(max.size) {
                val column = this@MaxProjectionOperator.parentColumns[it]
                when (column.type) {
                    Type.Byte -> ByteValue(max[it])
                    Type.Short -> ShortValue(max[it])
                    Type.Int -> IntValue(max[it])
                    Type.Long -> LongValue(max[it])
                    Type.Float -> FloatValue(max[it])
                    Type.Double -> DoubleValue(max[it])
                    else -> throw ExecutionException.OperatorExecutionException(this@MaxProjectionOperator, "The provided column $column cannot be used for a MAX projection.")
                }
            }

            /** Emit record. */
            emit(StandaloneRecord(0L, this@MaxProjectionOperator.columns, results))
        }
    }
}