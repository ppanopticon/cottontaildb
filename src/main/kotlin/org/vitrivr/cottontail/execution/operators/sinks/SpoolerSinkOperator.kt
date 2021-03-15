package org.vitrivr.cottontail.execution.operators.sinks

import io.grpc.stub.StreamObserver
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import org.vitrivr.cottontail.database.queries.binding.extensions.toLiteral
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.recordset.StandaloneRecord

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
class SpoolerSinkOperator(parent: Operator, private val queryId: String, private val index: Int, private val responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>? = null) : Operator.SinkOperator(parent) {

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

        val columns = this@SpoolerSinkOperator.parent.columns.map {
            val name = CottontailGrpc.ColumnName.newBuilder().setName(it.name.simple)
            val entityName = it.name.entity()
            if (entityName != null) {
                name.setEntity(CottontailGrpc.EntityName.newBuilder().setName(entityName.simple).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(entityName.schema().simple)))
            }
            name.build()
        }

        return flow {
            val responseBuilder = CottontailGrpc.QueryResponseMessage.newBuilder().setTid(CottontailGrpc.TransactionId.newBuilder().setQueryId(this@SpoolerSinkOperator.queryId).setValue(context.txId)).addAllColumns(columns)
            var accumulatedSize = 0L
            parent.onEach {
                val tuple = it.toTuple()
                if (accumulatedSize + tuple.serializedSize >= MAX_PAGE_SIZE_BYTES) {
                    this@SpoolerSinkOperator.responseObserver?.onNext(responseBuilder.build())
                    responseBuilder.clearTuples()
                    accumulatedSize = 0L
                }

                /* Add entry to page and increment counter. */
                responseBuilder.addTuples(tuple)
                accumulatedSize += tuple.serializedSize
            }.collect()

            /* Flush remaining tuples. */
            this@SpoolerSinkOperator.responseObserver?.onNext(responseBuilder.build())

            /* Signal completion. */
            emit(StandaloneRecord(TupleId.MIN_VALUE, emptyArray(), emptyArray()))
        }
    }


    /**
     *
     */
    private fun Record.toTuple(): CottontailGrpc.QueryResponseMessage.Tuple {
        val tuple = CottontailGrpc.QueryResponseMessage.Tuple.newBuilder()
        this.forEach { _, v -> tuple.addData(v?.toLiteral() ?: CottontailGrpc.Literal.newBuilder().setNullData(CottontailGrpc.Null.getDefaultInstance()).build()) }
        return tuple.build()
    }
}
