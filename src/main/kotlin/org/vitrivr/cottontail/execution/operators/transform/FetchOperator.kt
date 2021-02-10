package org.vitrivr.cottontail.execution.operators.transform

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An [Operator.PipelineOperator] used during query execution. Fetches the specified [ColumnDef] from
 * the specified [Entity]. Can  be used for late population of requested [ColumnDef]s.
 *
 * @author Ralph Gasser
 * @version 1.0.2
 */
class FetchOperator(parent: Operator, val entity: Entity, val fetch: Array<ColumnDef<*>>) : Operator.PipelineOperator(parent) {

    /** Columns returned by [FetchOperator] are a combination of the parent and the [FetchOperator]'s columns */
    override val columns: Array<ColumnDef<*>> = (this.parent.columns + this.fetch)

    /** [FetchOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [FetchOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [FetchOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val tx = context.getTx(this.entity) as EntityTx
        val buffer = ArrayList<Value?>(this.columns.size)
        return this.parent.toFlow(context).map { r ->
            buffer.clear()
            r.forEach { _, v -> buffer.add(v) }
            tx.read(r.tupleId, this.fetch).forEach { _, v -> buffer.add(v) }
            StandaloneRecord(r.tupleId, this.columns, buffer.toTypedArray())
        }
    }
}