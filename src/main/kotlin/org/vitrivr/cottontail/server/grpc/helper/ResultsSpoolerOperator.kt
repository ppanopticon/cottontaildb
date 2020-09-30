package org.vitrivr.cottontail.server.grpc.helper

import io.grpc.stub.StreamObserver
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.basics.SinkOperator
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord

/**
 * A [SinkOperator] that spools the results produced by the parent operator to the gRPC [StreamObserver].
 *
 * @param parent [Operator] that produces the results.
 * @param context [ExecutionEngine.ExecutionContext] the [ExecutionEngine.ExecutionContext]
 * @param queryId The ID of the query that produced the results
 * @param index Optional index of the result (for batched queries).
 * @param responseObserver [StreamObserver] used to send back the results.
 */
class ResultsSpoolerOperator(parent: Operator, context: ExecutionEngine.ExecutionContext, val queryId: String, val index: Int, val responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) : SinkOperator(parent, context) {
    /** The [ColumnDef]s returned by this [ResultsSpoolerOperator]. */
    override val columns: Array<ColumnDef<*>> = this.parent.columns

    /* Size of an individual results page based on the estimate of a single tuple's size. */
    private val pageSize: Int = StandaloneRecord(0L, this.columns, this.columns.map { it.defaultValue() }.toTypedArray()).let {
        (4_194_000_000 / recordToTuple(it).build().serializedSize).toInt()
    }

    /**
     * Called when [ResultsSpoolerOperator] is opened.
     */
    override fun prepareOpen() { /* No Op. */
    }

    /**
     * Called when [ResultsSpoolerOperator] is closed. Sends the last results and completes transmission.
     */
    override fun prepareClose() { /* No Op. */
    }

    override fun toFlow(scope: CoroutineScope): Flow<Record> {
        val parent = this.parent.toFlow(scope)

        /* Number of tuples returned so far. */
        var counter = 0

        /* Number of tuples returned so far. */
        var responseBuilder = CottontailGrpc.QueryResponseMessage.newBuilder().setQueryId(this.queryId)

        return flow {
            parent.collect {
                val tuple = recordToTuple(it)
                if (counter % this@ResultsSpoolerOperator.pageSize == 0) {
                    this@ResultsSpoolerOperator.responseObserver.onNext(responseBuilder.build())
                    responseBuilder = CottontailGrpc.QueryResponseMessage.newBuilder().setQueryId(this@ResultsSpoolerOperator.queryId)
                }

                /* Add entry to page and increment counter. */
                responseBuilder.addResults(tuple)
                counter++
            }

            /* Flush remaining tuples. */
            if (responseBuilder.resultsList.size > 0) {
                this@ResultsSpoolerOperator.responseObserver.onNext(responseBuilder.build())
            }

            /* Signal completion. */
            emit(StandaloneRecord(0L, emptyArray(), emptyArray()))
        }
    }

    /**
     * Generates a new [CottontailGrpc.Tuple.Builder] from a given [Record].
     *
     * @param record [Record] to create a [CottontailGrpc.Tuple.Builder] from.
     * @return Resulting [CottontailGrpc.Tuple.Builder]
     */
    private fun recordToTuple(record: Record): CottontailGrpc.Tuple.Builder = CottontailGrpc.Tuple
            .newBuilder().putAllData(record.toMap().map {
                it.key.toString() to (it.value?.toData()
                        ?: CottontailGrpc.Data.newBuilder().setNullData(CottontailGrpc.Null.getDefaultInstance()).build())
            }.toMap())
}