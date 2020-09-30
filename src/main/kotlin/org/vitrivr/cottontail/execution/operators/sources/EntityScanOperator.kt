package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.basics.OperatorStatus
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [AbstractEntityOperator] that scans an [Entity] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.1.3
 */
class EntityScanOperator(context: ExecutionEngine.ExecutionContext, entity: Entity, override val columns: Array<ColumnDef<*>>, private val range: LongRange = 1L..entity.statistics.maxTupleId) : AbstractEntityOperator(context, entity, columns) {
    /**
     * Converts this [EntityScanOperator] to a [Flow] and returns it.
     *
     * @param scope The [CoroutineScope] used for execution
     * @return [Flow] representing this [EntityScanOperator]
     * @throws IllegalStateException If this [Operator.status] is not [OperatorStatus.OPEN]
     */
    override fun toFlow(scope: CoroutineScope): Flow<Record> {
        check(this.status == OperatorStatus.OPEN) { "Cannot convert operator $this to flow because it is in state ${this.status}." }
        val tx = this.context.getTx(this.entity)
        return flow {
            tx.scan(this@EntityScanOperator.columns, this@EntityScanOperator.range).use { iterator ->
                for (record in iterator) {
                    emit(record)
                }
            }
        }
    }
}