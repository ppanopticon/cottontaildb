package org.vitrivr.cottontail.execution.operators.utility

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Record

/**
 * This is a [Operator.SourceOperator] that throws a specifiable [Throwable].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ThrowingSourceOperator(val t: Throwable) : Operator.SourceOperator() {
    override val columns: Array<ColumnDef<*>> = emptyArray()

    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        throw this@ThrowingSourceOperator.t
    }
}