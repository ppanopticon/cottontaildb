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

/**
 * An [Operator.PipelineOperator] used during query execution. It calculates the SUM of all values it
 * has encountered and returns it as a [Record].
 *
 * Only produces a single [Record] and converts the projected columns to a [Type.Double] column.
 * Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class SumProjectionOperator(
    parent: Operator,
    fields: List<Pair<Name.ColumnName, Name.ColumnName?>>
) : Operator.PipelineOperator(parent) {
    /** [SumProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /** Columns produced by [SumProjectionOperator]. */
    override val columns: Array<ColumnDef<*>> = this.parent.columns.mapNotNull { c ->
        val match = fields.find { f -> f.first.matches(c.name) }
        if (match != null) {
            if (!c.type.numeric) throw OperatorSetupException(
                this,
                "The provided column $match cannot be used for a ${Projection.SUM} projection because it has the wrong type."
            )
            val alias = match.second
            if (alias != null) {
                ColumnDef(alias, Type.Double)
            } else {
                val columnNameStr = "${Projection.SUM.label()}_${c.name.simple})"
                val columnName =
                    c.name.entity()?.column(columnNameStr) ?: Name.ColumnName(columnNameStr)
                ColumnDef(columnName, Type.Double)
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
     * Converts this [SumProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [SumProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val parentFlow = this.parent.toFlow(context)
        return flow {
            /* Prepare holder of type double. */
            val sum = this@SumProjectionOperator.parentColumns.map { 0.0 }.toTypedArray()
            parentFlow.onEach {
                this@SumProjectionOperator.parentColumns.forEachIndexed { i, c ->
                    sum[i] += when (val value = it[c]) {
                        is ByteValue -> value.value.toDouble()
                        is ShortValue -> value.value.toDouble()
                        is IntValue -> value.value.toDouble()
                        is LongValue -> value.value.toDouble()
                        is FloatValue -> value.value.toDouble()
                        is DoubleValue -> value.value
                        null -> 0.0
                        else -> throw ExecutionException.OperatorExecutionException(this@SumProjectionOperator, "The provided column $c cannot be used for a ${Projection.SUM} projection. ")
                    }
                }
            }.collect()

            /** Emit record. */
            val results = Array<Value?>(sum.size) { DoubleValue(sum[it]) }
            emit(StandaloneRecord(0L, this@SumProjectionOperator.columns, results))
        }
    }
}