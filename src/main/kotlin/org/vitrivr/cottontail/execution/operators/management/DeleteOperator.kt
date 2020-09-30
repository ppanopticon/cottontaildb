package org.vitrivr.cottontail.execution.operators.management

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.basics.OperatorStatus
import org.vitrivr.cottontail.execution.operators.basics.PipelineBreaker
import org.vitrivr.cottontail.execution.operators.predicates.FilterOperator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.LongValue
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * A [PipelineBreaker] that deletes all entries in an [Entity] that it receives.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class DeleteOperator(parent: Operator, context: ExecutionEngine.ExecutionContext, val entity: Entity) : PipelineBreaker(parent, context) {
    /** Columns returned by [DeleteOperator]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
            ColumnDef.withAttributes(Name.ColumnName("deleted"), "LONG", -1, false),
            ColumnDef.withAttributes(Name.ColumnName("duration_ms"), "DOUBLE", -1, false),
    )

    override fun prepareOpen() {
        this.context.prepareTransaction(this.entity, false)
    }

    override fun prepareClose() {
        /* NoOp. */
    }

    /**
     * Converts this [FilterOperator] to a [Flow] and returns it.
     *
     * @param scope The [CoroutineScope] used for execution
     * @return [Flow] representing this [FilterOperator]
     *
     * @throws IllegalStateException If this [Operator.status] is not [OperatorStatus.OPEN]
     */
    @ExperimentalTime
    override fun toFlow(scope: CoroutineScope): Flow<Record> {
        var deleted = 0L
        val parent = this.parent.toFlow(scope)
        val tx = this.context.getTx(this.entity)
        return flow {
            val time = measureTime {
                parent.collect {
                    tx.delete(it.tupleId) /* Safe, cause tuple IDs are retained for simple queries. */
                    deleted += 1
                }
            }
            tx.commit()
            emit(StandaloneRecord(0L, this@DeleteOperator.columns, arrayOf(LongValue(deleted), DoubleValue(time.inMilliseconds))))
        }
    }
}