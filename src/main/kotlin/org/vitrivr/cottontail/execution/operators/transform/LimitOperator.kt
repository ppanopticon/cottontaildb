package org.vitrivr.cottontail.execution.operators.transform

import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.*
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [Operator.PipelineOperator] used during query execution. Limit the number of outgoing [Record]s.
 *
 * @author Ralph Gasser
 * @version 1.1.2
 */
class LimitOperator(parent: Operator, val skip: Long, val limit: Long) : Operator.PipelineOperator(parent) {

    /** Columns returned by [LimitOperator] depend on the parent [Operator]. */
    override val columns: Array<ColumnDef<*>> = this.parent.columns

    /** [LimitOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [LimitOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [LimitOperator]
     * @throws IllegalStateException If this [Operator.status] is not [OperatorStatus.OPEN]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> = this.parent.toFlow(context).drop(this.skip).take(this.limit)
}