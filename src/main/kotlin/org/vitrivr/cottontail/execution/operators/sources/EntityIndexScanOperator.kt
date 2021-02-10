package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [AbstractEntityOperator] that scans an [Entity.Index] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class EntityIndexScanOperator(val index: Index, private val predicate: Predicate) : AbstractEntityOperator(index.parent, index.produces) {
    /**
     * Converts this [EntityIndexScanOperator] to a [Flow] and returns it.
     *
     * @param context The [TODO] used for execution.
     * @return [Flow] representing this [EntityIndexScanOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val tx = context.getTx(this.entity) as EntityTx
        val indexTx = context.getTx(tx.indexForName(this.index.name)) as IndexTx
        return flow {
            indexTx.filter(this@EntityIndexScanOperator.predicate).use { iterator ->
                for (record in iterator) {
                    emit(record)
                }
            }
        }
    }
}