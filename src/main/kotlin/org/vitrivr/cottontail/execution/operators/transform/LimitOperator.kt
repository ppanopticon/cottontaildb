package org.vitrivr.cottontail.execution.operators.transform

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.*
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [Operator.PipelineBreaker] that can be used to limit the number of incoming [Record]s.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class LimitOperator(parent: Operator, context: ExecutionEngine.ExecutionContext, val skip: Long, val limit: Long) : PipelineOperator(parent, context) {

    /** Columns returned by [LimitOperator] depend on the parent [Operator]. */
    override val columns: Array<ColumnDef<*>> = this.parent.columns

    override fun prepareOpen() { /* NoOp. */
    }

    override fun prepareClose() { /* NoOp. */
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
        return this.parent.toFlow(scope).drop(this.skip).take(this.limit)
    }
}