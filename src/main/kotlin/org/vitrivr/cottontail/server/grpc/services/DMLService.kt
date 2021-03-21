package org.vitrivr.cottontail.server.grpc.services

import com.google.protobuf.Empty
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.GrpcQueryBinder
import org.vitrivr.cottontail.database.queries.planning.CottontailQueryPlanner
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.management.InsertPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.logical.LeftConjunctionRewriteRule
import org.vitrivr.cottontail.database.queries.planning.rules.logical.RightConjunctionRewriteRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.index.BooleanIndexScanRule
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.execution.operators.sinks.SpoolerSinkOperator
import org.vitrivr.cottontail.execution.operators.utility.NoOpSinkOperator
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DMLGrpc
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.ExecutionException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.exceptions.TransactionException
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Implementation of [DMLGrpc.DMLImplBase], the gRPC endpoint for inserting data into Cottontail DB [DefaultEntity]s.
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
@ExperimentalTime
class DMLService(val catalogue: DefaultCatalogue, override val manager: TransactionManager) : DMLGrpc.DMLImplBase(), TransactionService {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(DMLService::class.java)
    }

    /** [GrpcQueryBinder] used to bind a gRPC query to a tree of node expressions. */
    private val binder = GrpcQueryBinder(this.catalogue)

    /** [CottontailQueryPlanner] instance used to generate execution plans from query definitions. */
    private val planner = CottontailQueryPlanner(
        logicalRules = listOf(LeftConjunctionRewriteRule, RightConjunctionRewriteRule),
        physicalRules = listOf(BooleanIndexScanRule),
        this.catalogue.config.cache.planCacheSize
    )

    /**
     * gRPC endpoint for handling UPDATE queries.
     */
    override fun update(request: CottontailGrpc.UpdateMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId, responseObserver) { tx, q ->
        try {
            val ctx = QueryContext(tx)
            val totalDuration = measureTime {
                /* Bind query and create logical plan. */
                this.binder.bind(request, ctx)

                /* Generate physical execution plan for query. */
                this.planner.planAndSelect(ctx)

                /* Execute UPDATE. */
                tx.execute(SpoolerSinkOperator(ctx.toOperatorTree(tx), q, 0, responseObserver))
            }

            /* Finalize invocation. */
            Status.OK.withDescription(formatMessage(tx, q, "Executing UPDATE took $totalDuration."))
        } catch (e: QueryException.QuerySyntaxException) {
            Status.INVALID_ARGUMENT.withDescription(formatMessage(tx, q, "UPDATE failed due to syntax error: ${e.message}"))
        } catch (e: QueryException.QueryBindException) {
            Status.INVALID_ARGUMENT.withDescription(formatMessage(tx, q, "UPDATE failed due to syntax error: ${e.message}"))
        } catch (e: TransactionException.DeadlockException) {
            Status.ABORTED.withDescription(formatMessage(tx, q, "UPDATE failed due to deadlock with other transaction: ${e.message}"))
        } catch (e: QueryException.QueryPlannerException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "UPDATE failed because of an error during query planning: ${e.message}")).withCause(e)
        } catch (e: DatabaseException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "DELETE failed due to database error: ${e.message}")).withCause(e)
        } catch (e: ExecutionException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "UPDATE failed due to execution error: ${e.message}")).withCause(e)
        } catch (e: Throwable) {
            Status.UNKNOWN.withDescription(formatMessage(tx, q, "UPDATE failed due an unhandled error: ${e.message}")).withCause(e)
        }
    }

    /**
     * gRPC endpoint for handling DELETE queries.
     */
    override fun delete(request: CottontailGrpc.DeleteMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId, responseObserver) { tx, q ->
        try {
            val ctx = QueryContext(tx)
            val totalDuration = measureTime {
                /* Bind query and create logical plan. */
                this.binder.bind(request, ctx)

                /* Generate physical execution plan for query. */
                this.planner.planAndSelect(ctx)

                /* Execute DELETE. */
                tx.execute(SpoolerSinkOperator(ctx.toOperatorTree(tx), q, 0, responseObserver))
            }

            /* Finalize invocation. */
            Status.OK.withDescription(formatMessage(tx, q, "Executed DELETE in $totalDuration."))
        } catch (e: QueryException.QuerySyntaxException) {
            Status.INVALID_ARGUMENT.withDescription(formatMessage(tx, q, "DELETE failed due to syntax error: ${e.message}"))
        } catch (e: QueryException.QueryBindException) {
            Status.INVALID_ARGUMENT.withDescription(formatMessage(tx, q, "DELETE failed due to binding error: ${e.message}"))
        } catch (e: TransactionException.DeadlockException) {
            Status.ABORTED.withDescription(formatMessage(tx, q, "DELETE failed due to deadlock with other transaction: ${e.message}"))
        } catch (e: QueryException.QueryPlannerException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "DELETE failed because of an error during query planning: ${e.message}")).withCause(e)
        } catch (e: DatabaseException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "DELETE failed due to database error: ${e.message}")).withCause(e)
        } catch (e: ExecutionException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "DELETE failed due to execution error: ${e.message}")).withCause(e)
        } catch (e: Throwable) {
            Status.UNKNOWN.withDescription(formatMessage(tx, q, "DELETE failed due an unexpected error: ${e.message}")).withCause(e)
        }
    }

    /**
     * gRPC endpoint for handling INSERT queries.
     */
    override fun insert(request: CottontailGrpc.InsertMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) = this.withTransactionContext(request.txId, responseObserver) { tx, q ->
        try {
            val ctx = QueryContext(tx)
            val totalDuration = measureTime {
                /* Bind query and create logical plan. */
                this.binder.bind(request, ctx)

                /* Generate physical execution plan for query.. */
                this.planner.planAndSelect(ctx)

                /* Execute INSERT. */
                val op = SpoolerSinkOperator(ctx.toOperatorTree(tx), q, 0, responseObserver)
                tx.execute(op)
            }

            /* Finalize invocation. */
            Status.OK.withDescription(formatMessage(tx, q, "Executed INSERT in $totalDuration."))
        } catch (e: QueryException.QuerySyntaxException) {
            Status.INVALID_ARGUMENT.withDescription(formatMessage(tx, q, "INSERT failed due to syntax error: ${e.message}"))
        } catch (e: QueryException.QueryBindException) {
            Status.INVALID_ARGUMENT.withDescription(formatMessage(tx, q, "INSERT failed due to binding error: ${e.message}"))
        } catch (e: TransactionException.DeadlockException) {
            Status.ABORTED.withDescription(formatMessage(tx, q, "INSERT failed due to deadlock with other transaction: ${e.message}"))
        } catch (e: QueryException.QueryPlannerException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "INSERT failed because of an error during query planning: ${e.message}")).withCause(e)
        } catch (e: DatabaseException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "INSERT failed due to database error: ${e.message}")).withCause(e)
        } catch (e: ExecutionException) {
            Status.INTERNAL.withDescription(formatMessage(tx, q, "INSERT failed due to execution error: ${e.message}")).withCause(e)
        } catch (e: Throwable) {
            Status.UNKNOWN.withDescription(formatMessage(tx, q, "INSERT failed due an unexpected error: ${e.message}")).withCause(e)
        }
    }


    /**
     *
     */
    override fun insertBatch(responseObserver: StreamObserver<Empty>): StreamObserver<CottontailGrpc.InsertMessage> = object : StreamObserver<CottontailGrpc.InsertMessage> {

        /** The [QueryContext] used for this INSERT transaction. */
        private var queryContext: QueryContext? = null

        /* Start new transaction; BATCH INSERTS are always executed in a single transaction. */
        private val transaction = this@DMLService.manager.Transaction(TransactionType.USER_IMPLICIT)

        /* Start new transaction; BATCH INSERTS are always executed in a single transaction. */
        private val queryId = UUID.randomUUID().toString()

        /** Processes the individual [CottontailGrpc.InsertMessage]s; these are merely cached in memory until [onCompleted] is called. */
        override fun onNext(value: CottontailGrpc.InsertMessage) {
            try {
                val localContext = this.queryContext
                if (localContext == null) {
                    val newQueryContext = QueryContext(this.transaction)
                    val bindTime = measureTime {
                        this@DMLService.binder.bind(value, newQueryContext)
                    }
                    LOGGER.debug(formatMessage(this.transaction, this.queryId, "Parsing & binding INSERT took $bindTime."))

                    /* Plan INSERT. */
                    val planTime = measureTime {
                        this@DMLService.planner.planAndSelect(newQueryContext, bypassCache = true, cache = false)
                    }
                    LOGGER.debug(formatMessage(this.transaction, this.queryId, "Planning INSERT took $planTime."))
                    this.queryContext = newQueryContext
                } else {
                    val bindTime = measureTime {
                        val binding = this@DMLService.binder.bindValues(value, localContext)
                        (localContext.physical as InsertPhysicalOperatorNode).records.add(binding)
                    }
                    LOGGER.debug(formatMessage(this.transaction, this.queryId, "Parsing & binding INSERT took $bindTime."))

                    if ((localContext.physical as InsertPhysicalOperatorNode).records.size >= this@DMLService.catalogue.config.cache.insertCacheSize) {
                        this.flush()
                    }
                }
                responseObserver.onNext(Empty.getDefaultInstance())
            } catch (e: QueryException.QuerySyntaxException) {
                val message = formatMessage(this.transaction, this.queryId, "INSERT failed due to syntax error: ${e.message}")
                LOGGER.info(message)
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException())
            } catch (e: QueryException.QueryBindException) {
                val message = formatMessage(this.transaction, this.queryId, "INSERT failed due to binding error: ${e.message}")
                LOGGER.info(message)
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException())
            } catch (e: QueryException.QueryPlannerException) {
                val message = formatMessage(this.transaction, this.queryId, "INSERT failed because of an error during query planning: ${e.message}")
                LOGGER.info(message)
                responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            } catch (e: TransactionException.DeadlockException) {
                val message = formatMessage(this.transaction, this.queryId, "INSERT failed due to deadlock with other transaction: ${e.message}")
                LOGGER.info(message)
                responseObserver.onError(Status.ABORTED.withDescription(message).asException())
            } catch (e: ExecutionException) {
                val message = formatMessage(this.transaction, this.queryId, "INSERT failed due to execution error: ${e.message}")
                LOGGER.info(message)
                responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            } catch (e: Throwable) {
                val message = formatMessage(this.transaction, this.queryId, "INSERT failed due an unhandled error: ${e.message}")
                LOGGER.error(message, e)
                responseObserver.onError(Status.UNKNOWN.withDescription(message).asException())
            }
        }

        /**
         *
         */
        override fun onError(t: Throwable) {
            this.transaction.rollback()
            LOGGER.info(formatMessage(this.transaction, this.queryId, "INSERT was aborted by the user! Force transaction to ROLLBACK."))
            responseObserver.onCompleted()
        }

        /**
         *
         */
        override fun onCompleted() {
            try {
                this.flush()
            } catch (e: ExecutionException) {
                val message = formatMessage(this.transaction, this.queryId, "INSERT failed due to execution error: ${e.message}")
                LOGGER.info(message)
                responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            } catch (e: Throwable) {
                val message = formatMessage(this.transaction, this.queryId, "INSERT failed due an unhandled error: ${e.message}")
                LOGGER.error(message, e)
                responseObserver.onError(Status.UNKNOWN.withDescription(message).asException())
            }

            /* Complete. */
            this.transaction.commit()
            responseObserver.onCompleted()
        }

        private fun flush() {
            val localContext = this.queryContext
            if (localContext != null) {
                val records = (localContext.physical as InsertPhysicalOperatorNode).records.size
                val duration = measureTime {
                    this.transaction.execute(NoOpSinkOperator(localContext.toOperatorTree(this.transaction)))
                    this.queryContext = null
                }
                val message = formatMessage(
                    this.transaction,
                    this.queryId,
                    "Executed BATCHED INSERT for $records records in $duration."
                )
                LOGGER.info(message)
            }
        }
    }
}
