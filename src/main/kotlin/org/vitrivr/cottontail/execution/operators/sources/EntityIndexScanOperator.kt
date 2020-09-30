package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.exceptions.ExecutionException
import org.vitrivr.cottontail.execution.operators.basics.OperatorStatus
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord

/**
 * An [AbstractEntityOperator] that scans an [Entity.Index] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.1.2
 */
class EntityIndexScanOperator(context: ExecutionEngine.ExecutionContext, entity: Entity, columns: Array<ColumnDef<*>>, private val predicate: BooleanPredicate, val indexHint: IndexType) : AbstractEntityOperator(context, entity, columns) {

    /** Get [Index] that */
    private val index = this.entity.allIndexes().filter { it.type == this.indexHint && it.canProcess(this.predicate) }.first()

    /** Determine [ColumnDef]s that are not produced by the [Index] and require separate fetching. */
    private val columnsToFetch = super.columns.filter { !this.index.produces.contains(it) }.toTypedArray()

    /** Determine [ColumnDef] produced by this [EntityIndexScanOperator]. */
    override val columns: Array<ColumnDef<*>> = this.index.produces + this.columnsToFetch

    override fun toFlow(scope: CoroutineScope): Flow<Record> {
        check(this.status == OperatorStatus.OPEN) { "Cannot convert operator $this to flow because it is in state ${this.status}." }
        val tx = this.context.getTx(this.entity)
        val indexTx = tx.index(this.index.name)
                ?: throw ExecutionException("Could not find desired index ${this.index.name}.")

        return flow {
            indexTx.filter(this@EntityIndexScanOperator.predicate).use { iterator ->
                for (record in iterator) {
                    if (columnsToFetch.isEmpty()) {
                        emit(record)
                    } else {
                        val fetched = tx.read(record.tupleId, columnsToFetch)
                        emit(StandaloneRecord(record.tupleId, this@EntityIndexScanOperator.columns, (record.values + fetched.values)))
                    }
                }
            }
        }
    }
}