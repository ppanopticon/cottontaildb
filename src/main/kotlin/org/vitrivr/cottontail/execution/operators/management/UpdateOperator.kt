package org.vitrivr.cottontail.execution.operators.management

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.Type
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.model.values.types.Value
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * An [Operator.PipelineOperator] used during query execution. Updates all entries in an [Entity]
 * that it receives with the provided [Value].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class UpdateOperator(parent: Operator, val entity: Entity, val values: List<Pair<ColumnDef<*>, Value?>>) : Operator.PipelineOperator(parent) {

    companion object {
        /** The columns produced by the [UpdateOperator]. */
        val COLUMNS: Array<ColumnDef<*>> = arrayOf(
            ColumnDef(Name.ColumnName("updated"), Type.Long, false),
            ColumnDef(Name.ColumnName("duration_ms"), Type.Double, false)
        )
    }

    /** Columns produced by [UpdateOperator]. */
    override val columns: Array<ColumnDef<*>> = COLUMNS

    /** [UpdateOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [UpdateOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [UpdateOperator]
     */
    @ExperimentalTime
    override fun toFlow(context: TransactionContext): Flow<Record> {
        var updated = 0L
        val parent = this.parent.toFlow(context)
        val tx = context.getTx(this.entity) as Entity.Tx
        return flow {
            val time = measureTime {
                parent.collect { record ->
                    for (value in this@UpdateOperator.values) {
                        record[value.first] = value.second
                    }
                    tx.update(record) /* Safe, cause tuple IDs are retained for simple queries. */
                    updated += 1
                }
            }
            emit(StandaloneRecord(0L, this@UpdateOperator.columns, arrayOf(LongValue(updated), DoubleValue(time.inMilliseconds))))
        }
    }
}