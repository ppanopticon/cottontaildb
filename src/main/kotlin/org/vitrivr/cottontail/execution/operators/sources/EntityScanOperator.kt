package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [AbstractEntityOperator] that scans an [Entity] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.3.1
 */
class EntityScanOperator(groupId: GroupId, entity: EntityTx, columns: Array<ColumnDef<*>>, private val partitionIndex: Int, private val partitions: Int) : AbstractEntityOperator(groupId, entity, columns) {
    /**
     * Converts this [EntityScanOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [EntityScanOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        return flow {
            for (record in this@EntityScanOperator.entity.scan(this@EntityScanOperator.columns, this@EntityScanOperator.partitionIndex, this@EntityScanOperator.partitions)) {
                emit(record)
            }
        }
    }
}