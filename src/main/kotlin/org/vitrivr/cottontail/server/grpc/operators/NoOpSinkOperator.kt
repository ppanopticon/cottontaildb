package org.vitrivr.cottontail.server.grpc.operators

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.recordset.StandaloneRecord

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class NoOpSinkOperator(parent: Operator) : Operator.SinkOperator(parent) {
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val parent = this.parent.toFlow(context)
        return flow {
            parent.collect {

            }

            /* Signal completion. */
            emit(StandaloneRecord(TupleId.MIN_VALUE, emptyArray(), emptyArray()))
        }
    }
}