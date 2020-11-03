package org.vitrivr.cottontail.execution.operators.transform

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.*
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord

/**
 * An [PipelineOperator] that fetches the specified [ColumnDef] from the specified [Entity]. Can
 * be used for late population of requested [ColumnDef]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FetchOperator(parent: Operator, context: ExecutionEngine.ExecutionContext, val entity: Entity, val fetch: Array<ColumnDef<*>>) : PipelineOperator(parent, context) {

    /** Columns returned by [FetchOperator] are a combination of the parent and the [FetchOperator]'s columns */
    override val columns: Array<ColumnDef<*>> = (this.parent.columns + this.fetch)

    override fun prepareOpen() {
        this.context.prepareTransaction(this.entity, true)
    }

    override fun prepareClose() {
        /* NoOp. */
    }

    /**
     * Converts this [LimitOperator] to a [Flow] and returns it.
     *
     * @param scope The [CoroutineScope] used for execution
     * @return [Flow] representing this [LimitOperator]
     * @throws IllegalStateException If this [Operator.status] is not [OperatorStatus.OPEN]
     */
    override fun toFlow(scope: CoroutineScope): Flow<Record> {
        check(this.status == OperatorStatus.OPEN) { "Cannot convert operator $this to flow because it is in state ${this.status}." }
        val tx = this.context.getTx(this.entity)
        return this.parent.toFlow(scope).map { r ->
            val fetched = tx.read(r.tupleId, this.fetch)
            StandaloneRecord(r.tupleId, this.columns, r.values + fetched.values)
        }
    }
}