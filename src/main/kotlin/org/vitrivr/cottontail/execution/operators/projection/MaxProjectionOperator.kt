package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.*
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.exceptions.OperatorSetupException
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.ExecutionException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.*
import kotlin.math.max

/**
 * A [Operator.PipelineOperator] used during query execution. It tracks the maximum value it has encountered
 * so far and returns it as a [Record]. The [MaxProjectionOperator] retains the type of the incoming records.
 *
 * Only produces a single [Record]. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.1.1
 */
class MaxProjectionOperator(parent: Operator, val name: Name.ColumnName, val alias: Name.ColumnName? = null) : Operator.PipelineOperator(parent) {

    /** The [ColumnDef] of the incoming [Operator] that is being used for calculation. */
    private val parentColumn: ColumnDef<*> = this.parent.columns.firstOrNull { it.name == this.name && it.type.numeric }
            ?: throw OperatorSetupException(this, "The provided column $name cannot be used for a MAX projection. It either doesn't exist or has the wrong type.")

    /** [MaxProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /** Columns produced by [MeanProjectionOperator]. */
    override val columns: Array<ColumnDef<*>> = when (this.parentColumn.type) {
        ByteColumnType -> arrayOf(
                ColumnDef.withAttributes(
                        this.alias
                                ?: (this.parentColumn.name.entity()?.column("min(${this.parentColumn.name.simple})")
                                        ?: Name.ColumnName("min(${this.parentColumn.name.simple}")),
                        "BYTE"
                )
        )
        ShortColumnType -> arrayOf(
                ColumnDef.withAttributes(
                        this.alias
                                ?: (this.parentColumn.name.entity()?.column("min(${this.parentColumn.name.simple})")
                                        ?: Name.ColumnName("min(${this.parentColumn.name.simple}")),
                        "SHORT"
                )
        )
        IntColumnType -> arrayOf(
                ColumnDef.withAttributes(
                        this.alias
                                ?: (this.parentColumn.name.entity()?.column("min(${this.parentColumn.name.simple})")
                                        ?: Name.ColumnName("min(${this.parentColumn.name.simple}")),
                        "INT"
                )
        )
        LongColumnType -> arrayOf(
                ColumnDef.withAttributes(
                        this.alias
                                ?: (this.parentColumn.name.entity()?.column("min(${this.parentColumn.name.simple})")
                                        ?: Name.ColumnName("min(${this.parentColumn.name.simple}")),
                        "LONG"
                )
        )
        FloatColumnType -> arrayOf(
                ColumnDef.withAttributes(
                        this.alias
                                ?: (this.parentColumn.name.entity()?.column("min(${this.parentColumn.name.simple})")
                                        ?: Name.ColumnName("min(${this.parentColumn.name.simple}")),
                        "FLOAT"
                )
        )
        DoubleColumnType -> arrayOf(
                ColumnDef.withAttributes(
                        this.alias
                                ?: (this.parentColumn.name.entity()?.column("min(${this.parentColumn.name.simple})")
                                        ?: Name.ColumnName("min(${this.parentColumn.name.simple}")),
                        "DOUBLE"
                )
        )
        else -> throw OperatorSetupException(this, "The provided column $parentColumn type cannot be used for a MIN projection. ")
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
            var max = Double.MIN_VALUE
            parentFlow.collect {
                val value = it[parentColumn]
                when (value) {
                    is ByteValue -> max = max(max, value.value.toDouble())
                    is ShortValue -> max = max(max, value.value.toDouble())
                    is IntValue -> max = max(max, value.value.toDouble())
                    is LongValue -> max = max(max, value.value.toDouble())
                    is FloatValue -> max = max(max, value.value.toDouble())
                    is DoubleValue -> max = max(max, value.value)
                    else -> throw ExecutionException.OperatorExecutionException(this@MaxProjectionOperator, "The provided column $parentColumn cannot be used for a MAX projection. ")
                }
            }
            when (parentColumn.type) {
                ByteColumnType -> emit(StandaloneRecord(0L, this@MaxProjectionOperator.columns, arrayOf(ByteValue(max))))
                ShortColumnType -> emit(StandaloneRecord(0L, this@MaxProjectionOperator.columns, arrayOf(ShortValue(max))))
                IntColumnType -> emit(StandaloneRecord(0L, this@MaxProjectionOperator.columns, arrayOf(IntValue(max))))
                LongColumnType -> emit(StandaloneRecord(0L, this@MaxProjectionOperator.columns, arrayOf(LongValue(max))))
                FloatColumnType -> emit(StandaloneRecord(0L, this@MaxProjectionOperator.columns, arrayOf(FloatValue(max))))
                DoubleColumnType -> emit(StandaloneRecord(0L, this@MaxProjectionOperator.columns, arrayOf(DoubleValue(max))))
                else -> throw ExecutionException.OperatorExecutionException(this@MaxProjectionOperator, "The provided column $parentColumn cannot be used for a MAX projection. ")
            }
        }
    }
}