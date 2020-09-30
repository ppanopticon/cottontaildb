package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.exceptions.OperatorSetupException
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.basics.OperatorStatus
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import java.util.*

/**
 * An [AbstractEntityOperator] that samples an [Entity] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.1.2
 */
class EntitySampleOperator(context: ExecutionEngine.ExecutionContext, entity: Entity, columns: Array<ColumnDef<*>>, val size: Long, val seed: Long) : AbstractEntityOperator(context, entity, columns) {

    init {
        if (this.size <= 0L) throw OperatorSetupException(this, "EntitySampleOperator sample size is invalid (size=${this.size}).")
    }

    /**
     * Converts this [EntitySampleOperator] to a [Flow] and returns it.
     *
     * @param scope The [CoroutineScope] used for execution
     * @return [Flow] representing this [EntitySampleOperator]
     * @throws IllegalStateException If this [Operator.status] is not [OperatorStatus.OPEN]
     */
    override fun toFlow(scope: CoroutineScope): Flow<Record> {
        check(this.status == OperatorStatus.OPEN) { "Cannot convert operator $this to flow because it is in state ${this.status}." }
        val tx = this.context.getTx(this.entity)
        val random = SplittableRandom(this.seed)
        return flow {
            for (i in 0 until size) {
                var record: Record? = null
                while (record == null) {
                    val next = random.nextLong(tx.maxTupleId())
                    record = tx.read(next, this@EntitySampleOperator.columns)
                }
                emit(record)
            }
        }
    }
}