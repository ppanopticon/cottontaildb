package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.components.Predicate
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.exceptions.ExecutionException
import org.vitrivr.cottontail.execution.operators.basics.OperatorStatus
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [AbstractEntityOperator] that scans an [Entity.Index] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.1.4
 */
class EntityIndexScanOperator(context: ExecutionEngine.ExecutionContext, val index: Index, private val predicate: Predicate) : AbstractEntityOperator(context, index.parent, index.produces) {
    override fun toFlow(scope: CoroutineScope): Flow<Record> {
        check(this.status == OperatorStatus.OPEN) { "Cannot convert operator $this to flow because it is in state ${this.status}." }
        val tx = this.context.getTx(this.entity)
        val indexTx = tx.index(this.index.name)
                ?: throw ExecutionException("Could not find desired index ${this.index.name}.")

        return flow {
            indexTx.filter(this@EntityIndexScanOperator.predicate).use { iterator ->
                for (record in iterator) {
                    emit(record)
                }
            }
        }
    }
}