package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.exceptions.OperatorSetupException
import org.vitrivr.cottontail.model.basics.Record
import java.util.*

/**
 * An [AbstractEntityOperator] that samples an [Entity] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.2.1
 */
class EntitySampleOperator(groupId: GroupId, entity: EntityTx, columns: Array<ColumnDef<*>>, val size: Long, val seed: Long) : AbstractEntityOperator(groupId, entity, columns) {

    init {
        if (this.size <= 0L) throw OperatorSetupException(this, "EntitySampleOperator sample size is invalid (size=${this.size}).")
    }

    /**
     * Converts this [EntitySampleOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution.
     * @return [Flow] representing this [EntitySampleOperator].
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val random = SplittableRandom(this.seed)
        return flow {
            for (i in 0 until size) {
                var record: Record? = null
                while (record == null) {
                    val next = random.nextLong(this@EntitySampleOperator.entity.maxTupleId())
                    record = this@EntitySampleOperator.entity.read(next, this@EntitySampleOperator.columns)
                }
                emit(record)
            }
        }
    }
}