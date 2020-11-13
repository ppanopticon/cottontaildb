package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.Transaction
import org.vitrivr.cottontail.database.queries.planning.CottontailQueryPlanner
import org.vitrivr.cottontail.database.queries.planning.rules.logical.LeftConjunctionRewriteRule
import org.vitrivr.cottontail.database.queries.planning.rules.logical.RightConjunctionRewriteRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation.DeleteImplementationRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation.EntityScanImplementationRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation.FilterImplementationRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation.UpdateImplementationRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.index.BooleanIndexScanRule
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.exceptions.ExecutionException
import org.vitrivr.cottontail.grpc.CottonDMLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.exceptions.ValidationException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.server.grpc.helper.GrpcQueryBinder
import org.vitrivr.cottontail.server.grpc.helper.ResultsSpoolerOperator
import org.vitrivr.cottontail.server.grpc.helper.fqn
import org.vitrivr.cottontail.server.grpc.helper.toValue
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.StampedLock
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime
import kotlin.time.measureTimedValue

/**
 * Implementation of [CottonDMLGrpc.CottonDMLImplBase], the gRPC endpoint for inserting data into Cottontail DB [Entity]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
@ExperimentalTime
class CottonDMLService(val catalogue: Catalogue, val engine: ExecutionEngine) : CottonDMLGrpc.CottonDMLImplBase() {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(CottonDMLService::class.java)
    }

    /** [GrpcQueryBinder] used to generate [org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalNodeExpression] tree from a gRPC query. */
    private val binder = GrpcQueryBinder(this.catalogue)

    /** [CottontailQueryPlanner] used to generate execution plans from query definitions. */
    private val planner = CottontailQueryPlanner(
            logicalRewriteRules = listOf(LeftConjunctionRewriteRule, RightConjunctionRewriteRule),
            physicalRewriteRules = listOf(BooleanIndexScanRule, EntityScanImplementationRule, FilterImplementationRule, DeleteImplementationRule, UpdateImplementationRule)
    )

    /**
     * gRPC endpoint for handling UPDATE queries.
     */
    override fun update(request: CottontailGrpc.UpdateMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = try {
        /* Create a new execution context for the query. */
        val context = this.engine.ExecutionContext()
        val queryId = request.queryId.ifBlank { context.uuid.toString() }
        val totalDuration = measureTime {
            /* Bind query and create logical plan. */
            val bindTimedValue = measureTimedValue {
                this.binder.parseAndBindUpdate(request)
            }
            LOGGER.trace("Parsing & binding UPDATE $queryId took ${bindTimedValue.duration}.")

            /* Plan query and create execution plan. */
            val planningTime = measureTime {
                val candidates = this.planner.plan(bindTimedValue.value)
                if (candidates.isEmpty()) {
                    responseObserver.onError(Status.INTERNAL.withDescription("UPDATE query execution failed because no valid execution plan could be produced").asException())
                    return
                }
                val operator = candidates.minByOrNull { it.totalCost }!!.toOperator(context)
                context.addOperator(ResultsSpoolerOperator(operator, context, queryId, 0, responseObserver))
            }
            LOGGER.trace("Planning UPDATE $queryId took $planningTime.")

            /* Execute query. */
            context.execute()
        }

        /* Complete query. */
        responseObserver.onCompleted()
        LOGGER.trace("Executing UPDATE ${context.uuid} took $totalDuration to complete.")
    } catch (e: QueryException.QuerySyntaxException) {
        LOGGER.error("Error while executing UPDATE $request", e)
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("UPDATE syntax is invalid: ${e.message}").asException())
    } catch (e: QueryException.QueryBindException) {
        LOGGER.error("Error while executing UPDATE $request", e)
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("UPDATE query binding failed: ${e.message}").asException())
    } catch (e: ExecutionException) {
        LOGGER.error("Error while executing UPDATE $request", e)
        responseObserver.onError(Status.INTERNAL.withDescription("UPDATE execution failed: ${e.message}").asException())
    } catch (e: DatabaseException) {
        LOGGER.error("Error while executing UPDATE $request", e)
        responseObserver.onError(Status.INTERNAL.withDescription("UPDATE execution failed failed because of a database error: ${e.message}").asException())
    } catch (e: Throwable) {
        LOGGER.error("Error while executing UPDATE $request", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("UPDATE execution failed failed because of an unknown error: ${e.message}").asException())
    }

    /**
     * gRPC endpoint for handling DELETE queries.
     */
    override fun delete(request: CottontailGrpc.DeleteMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = try {
        /* Create a new execution context for the query. */
        val context = this.engine.ExecutionContext()
        val queryId = request.queryId.ifBlank { context.uuid.toString() }
        val totalDuration = measureTime {
            /* Bind query and create logical plan. */
            val bindTimedValue = measureTimedValue {
                this.binder.parseAndBindDelete(request)
            }
            LOGGER.trace("Parsing & binding DELETE $queryId took ${bindTimedValue.duration}.")

            /* Plan query and create execution plan. */
            val planningTime = measureTime {
                val candidates = this.planner.plan(bindTimedValue.value)
                if (candidates.isEmpty()) {
                    responseObserver.onError(Status.INTERNAL.withDescription("DELETE query execution failed because no valid execution plan could be produced").asException())
                    return
                }
                val operator = candidates.minByOrNull { it.totalCost }!!.toOperator(context)
                context.addOperator(ResultsSpoolerOperator(operator, context, queryId, 0, responseObserver))
            }
            LOGGER.trace("Planning DELETE $queryId took $planningTime.")

            /* Execute query. */
            context.execute()
        }

        /* Complete query. */
        responseObserver.onCompleted()
        LOGGER.trace("Executing DELETE ${context.uuid} took $totalDuration to complete.")
    } catch (e: QueryException.QuerySyntaxException) {
        LOGGER.error("Error while executing DELETE $request", e)
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("DELETE syntax is invalid: ${e.message}").asException())
    } catch (e: QueryException.QueryBindException) {
        LOGGER.error("Error while executing DELETE $request", e)
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("DELETE query binding failed: ${e.message}").asException())
    } catch (e: ExecutionException) {
        LOGGER.error("Error while executing DELETE $request", e)
        responseObserver.onError(Status.INTERNAL.withDescription("DELETE execution failed: ${e.message}").asException())
    } catch (e: DatabaseException) {
        LOGGER.error("Error while executing DELETE $request", e)
        responseObserver.onError(Status.INTERNAL.withDescription("DELETE execution failed failed because of a database error: ${e.message}").asException())
    } catch (e: Throwable) {
        LOGGER.error("Error while executing DELETE $request", e)
        responseObserver.onError(Status.UNKNOWN.withDescription("DELETE execution failed failed because of an unknown error: ${e.message}").asException())
    }

    /**
     * gRPC endpoint for inserting data in a streaming mode; transactions will stay open until the caller explicitly completes or until an error occurs.
     * As new entities are being inserted, new transactions will be created and thus new locks will be acquired.
     */
    override fun insert(responseObserver: StreamObserver<CottontailGrpc.Status>): StreamObserver<CottontailGrpc.InsertMessage> = InsertSink(responseObserver)

    /**
     * Class that acts as [StreamObserver] for [CottontailGrpc.InsertMessage]s.
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class InsertSink(private val responseObserver: StreamObserver<CottontailGrpc.Status>) : StreamObserver<CottontailGrpc.InsertMessage> {

        /** List of all the [Entity.Tx] associated with this call. */
        private val transactions = ConcurrentHashMap<Name.EntityName, Entity.Tx>()

        /** Generates a new transaction id. */
        private val txId = UUID.randomUUID()

        /** Lock for closing this */
        private val closeLock = StampedLock()

        /** Internal counter used to keep track of how many items were inserted. */
        @Volatile
        private var counter: Long = 0

        /** Flag indicating that call was closed. */
        @Volatile
        private var closed = false

        init {
            LOGGER.trace("Insert transaction {} was initiated by client.", txId.toString())
        }

        /**
         * Called whenever a new [CottontailGrpc.InsertMessage] arrives.
         *
         * @param request The next [CottontailGrpc.InsertMessage] message.
         */
        override fun onNext(request: CottontailGrpc.InsertMessage) {
            try {
                /* Check if call was closed and return. */
                this.closeLock.read {
                    if (this.closed) return
                    val fqn = try {
                        request.from.entity.fqn()
                    } catch (e: IllegalArgumentException) {
                        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to insert into entity: ${e.message}").asException())
                        return
                    }

                    /* Re-use or create Transaction. */
                    var tx = this.transactions[fqn]
                    if (tx == null) {
                        /* Extract required schema and entity. */
                        val entity = this@CottonDMLService.catalogue.instantiateEntity(fqn)
                        tx = entity.Tx(false, this.txId)
                        this.transactions[fqn] = tx
                    }

                    /* Execute insert action. */
                    val columns = ArrayList<ColumnDef<*>>(request.tuple.dataMap.size)
                    val values = ArrayList<Value?>(request.tuple.dataMap.size)
                    request.tuple.dataMap.forEach {
                        val col = tx.entity.columnForName(fqn.column(it.key))
                                ?: throw ValidationException("INSERT failed because column ${it.key} does not exist in entity '$fqn'.")
                        columns.add(col)
                        values.add(it.value.toValue(col))
                    }

                    /* Conduct INSERT. */
                    tx.insert(StandaloneRecord(columns = columns.toTypedArray(), values = values.toTypedArray()))

                    /* Increment counter. */
                    this.counter += 1

                    /* Respond with status. */
                    this.responseObserver.onNext(CottontailGrpc.Status.newBuilder().setSuccess(true).setTimestamp(System.currentTimeMillis()).build())
                }
            } catch (e: DatabaseException.SchemaDoesNotExistException) {
                this.cleanup(false)
                this.responseObserver.onError(Status.NOT_FOUND.withDescription("INSERT failed because schema '${request.from.entity.schema.name} does not exist!").asException())
            } catch (e: DatabaseException.EntityDoesNotExistException) {
                this.cleanup(false)
                this.responseObserver.onError(Status.NOT_FOUND.withDescription("INSERT failed because entity '${request.from.entity.fqn()} does not exist!").asException())
            } catch (e: DatabaseException.ColumnDoesNotExistException) {
                this.cleanup(false)
                this.responseObserver.onError(Status.NOT_FOUND.withDescription("INSERT failed because column '${e.column}' does not exist!").asException())
            } catch (e: ValidationException) {
                this.cleanup(false)
                this.responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("INSERT failed because data validation failed: ${e.message}").asException())
            } catch (e: DatabaseException) {
                this.cleanup(false)
                this.responseObserver.onError(Status.INTERNAL.withDescription("INSERT failed because of a database error: ${e.message}").asException())
            } catch (e: Throwable) {
                this.cleanup(false)
                this.responseObserver.onError(Status.UNKNOWN.withDescription("INSERT failed because of a unknown error: ${e.message}").asException())
            }
        }

        /**
         * Called when the client-side indicates an error condition.
         */
        override fun onError(t: Throwable) {
            this.cleanup(false)
            this.responseObserver.onError(Status.ABORTED.withDescription("Transaction was aborted by client.").asException())
        }

        /**
         * Called when client-side completes invocation, effectively ending the [Transaction].
         */
        override fun onCompleted() {
            this.cleanup(true)
            this.responseObserver.onCompleted()
        }

        /**
         * Commits or rolls back all the  [Transaction]s associated with this call and closes them afterwards.
         *
         * @param commit If true [Transaction]s are commited before closing. Otherwise, they are rolled back.
         */
        private fun cleanup(commit: Boolean = false) = this.closeLock.write {
            this.closed = true
            this.transactions.forEach {
                try {
                    if (commit) {
                        it.value.commit()
                        LOGGER.trace("Insert transaction ${this.txId} was committed by client (${this.counter} tuples inserted).")
                    } else {
                        LOGGER.trace("Insert transaction ${this.txId} was rolled back by client.")
                        it.value.rollback()
                    }
                } finally {
                    it.value.close()
                }
            }
        }
    }
}
