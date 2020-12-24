package org.vitrivr.cottontail.server.grpc.operators

import io.grpc.stub.StreamObserver
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow

import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.server.grpc.helper.toLiteral

/**
 * A [Operator.SinkOperator] used during query execution. Spools the results produced by the parent
 * operator to the gRPC [StreamObserver].
 *
 * @param parent [Operator] that produces the results.
 * @param queryId The ID of the query that produced the results
 * @param index Optional index of the result (for batched queries).
 * @param responseObserver [StreamObserver] used to send back the results.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class SpoolerSinkOperator(parent: Operator, val queryId: String, val index: Int, val responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) : Operator.SinkOperator(parent) {

    companion object {
        private const val MAX_PAGE_SIZE_BYTES = 4_000_000
    }

    /**
     * Converts this [SpoolerSinkOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [SpoolerSinkOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val parent = this.parent.toFlow(context)
        return flow {
            val columns = this@SpoolerSinkOperator.parent.columns.map {
                val name = CottontailGrpc.ColumnName.newBuilder().setName(it.name.simple)
                val entityName = it.name.entity()
                if (entityName != null) {
                    name.setEntity(CottontailGrpc.EntityName.newBuilder().setName(entityName.simple).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(entityName.schema().simple)))
                }
                name.build()
            }
            var responseBuilder = CottontailGrpc.QueryResponseMessage.newBuilder().setTid(CottontailGrpc.TransactionId.newBuilder().setQueryId(this@SpoolerSinkOperator.queryId).setValue(context.txId)).addAllColumns(columns)
            var accumulatedSize = 0L
            parent.collect {
                val tuple = it.toTuple()
                if (accumulatedSize + tuple.serializedSize >= MAX_PAGE_SIZE_BYTES) {
                    this@SpoolerSinkOperator.responseObserver.onNext(responseBuilder.build())
                    responseBuilder = CottontailGrpc.QueryResponseMessage.newBuilder().setTid(CottontailGrpc.TransactionId.newBuilder().setQueryId(this@SpoolerSinkOperator.queryId).setValue(context.txId)).addAllColumns(columns)
                    accumulatedSize = 0L
                }

                /* Add entry to page and increment counter. */
                responseBuilder.addTuples(tuple)
                accumulatedSize += tuple.serializedSize
            }

            /* Flush remaining tuples. */
            if (responseBuilder.tuplesList.size > 0) {
                this@SpoolerSinkOperator.responseObserver.onNext(responseBuilder.build())
            }

            /* Signal completion. */
            emit(StandaloneRecord(TupleId.MIN_VALUE, emptyArray(), emptyArray()))
        }
    }


    /**
     *
     */
    private fun Record.toTuple(): CottontailGrpc.QueryResponseMessage.Tuple {
        val tuple = CottontailGrpc.QueryResponseMessage.Tuple.newBuilder()
        this.forEach { _, v ->
            tuple.addData(v?.toLiteral()
                    ?: CottontailGrpc.Literal.newBuilder().setNullData(CottontailGrpc.Null.getDefaultInstance()).build())
        }
        return tuple.build()
    }
}
