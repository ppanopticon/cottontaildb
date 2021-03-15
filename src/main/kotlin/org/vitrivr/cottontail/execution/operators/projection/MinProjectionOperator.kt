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
import kotlin.math.min

/**
 * An [Operator.PipelineOperator] used during query execution. It tracks the MIN (minimum) value it
 * has encountered so far  and returns it as a [Record]. The [MinProjectionOperator] retains the type
 * of the incoming records.
 *
 * Only produces a single [Record]. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class MinProjectionOperator(
    parent: Operator,
    fields: List<Pair<Name.ColumnName, Name.ColumnName?>>
) : Operator.PipelineOperator(parent) {

    /** [MinProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /** Columns produced by [MinProjectionOperator]. */
    override val columns: Array<ColumnDef<*>> = this.parent.columns.mapNotNull { c ->
        val match = fields.find { f -> f.first.matches(c.name) }
        if (match != null) {
            if (!c.type.numeric) throw OperatorSetupException(
                this,
                "The provided column $match cannot be used for a ${Projection.MIN} projection because it has the wrong type."
            )
            val alias = match.second
            if (alias != null) {
                c.copy(name = alias)
            } else {
                val columnNameStr = "${Projection.MIN.label()}_${c.name.simple})"
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
     * Converts this [MinProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [MinProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val parentFlow = this.parent.toFlow(context)
        return flow {
            /* Prepare holder of type double, which can hold all types of values and collect incoming flow */
            val min = this@MinProjectionOperator.parentColumns.map { Double.MAX_VALUE }.toTypedArray()
            parentFlow.onEach {
                this@MinProjectionOperator.parentColumns.forEachIndexed { i, c ->
                    min[i] = when (val value = it[c]) {
                        is ByteValue -> min(min[i], value.value.toDouble())
                        is ShortValue -> min(min[i], value.value.toDouble())
                        is IntValue -> min(min[i], value.value.toDouble())
                        is LongValue -> min(min[i], value.value.toDouble())
                        is FloatValue -> min(min[i], value.value.toDouble())
                        is DoubleValue -> min(min[i], value.value)
                        null -> min[i]
                        else -> throw ExecutionException.OperatorExecutionException(this@MinProjectionOperator, "The provided column $c cannot be used for a MIN projection. ")
                    }
                }
            }.collect()

            /* Convert to original value type. */
            val results = Array<Value?>(min.size) {
                val column = this@MinProjectionOperator.parentColumns[it]
                when (column.type) {
                    Type.Boolean -> ByteValue(min[it])
                    Type.Short -> ShortValue(min[it])
                    Type.Int -> IntValue(min[it])
                    Type.Long -> LongValue(min[it])
                    Type.Float -> FloatValue(min[it])
                    Type.Double -> DoubleValue(min[it])
                    else -> throw ExecutionException.OperatorExecutionException(this@MinProjectionOperator, "The provided column $column cannot be used for a MIN projection.")
                }
            }

            /** Emit record. */
            emit(StandaloneRecord(0L, this@MinProjectionOperator.columns, results))
        }
    }
}