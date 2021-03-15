package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.LongValue

/**
 * An [AbstractEntityOperator] that counts the number of entries in an [Entity] and returns one [Record] with that number.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class EntityCountOperator(groupId: GroupId, entity: Entity) : AbstractEntityOperator(groupId, entity, arrayOf()) {

    /** The [ColumnDef] returned by this [EntitySampleOperator]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(entity.name.column(Projection.COUNT.label()), Type.Long)
    )

    /**
     * Converts this [EntityCountOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution.
     * @return [Flow] representing this [EntityCountOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val tx = context.getTx(this.entity) as EntityTx
        return flow {
            emit(StandaloneRecord(0L, this@EntityCountOperator.columns, arrayOf(LongValue(tx.count()))))
        }
    }
}