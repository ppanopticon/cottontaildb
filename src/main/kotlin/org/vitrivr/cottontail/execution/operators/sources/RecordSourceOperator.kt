package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [Operator.SourceOperator] that acts as a source for a single [Record]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class RecordSourceOperator(val records: List<Record>) : Operator.SourceOperator() {

    /** */
    override val columns = this.records.first().columns

    /**
     *
     */
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        this@RecordSourceOperator.records.forEach {
            emit(it)
        }
    }
}