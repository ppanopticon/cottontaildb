package org.vitrivr.cottontail.execution

import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.config.ExecutionConfig
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.execution.ExecutionEngine.ExecutionContext
import org.vitrivr.cottontail.execution.exceptions.ExecutionException
import org.vitrivr.cottontail.execution.exceptions.OperatorExecutionException
import org.vitrivr.cottontail.execution.operators.basics.SinkOperator
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

/**
 * The default [ExecutionEngine] for Cottontail DB. It hosts all the necessary facilities to create
 * and execute query [ExecutionContext]s.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class ExecutionEngine(config: ExecutionConfig) {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(ExecutionEngine::class.java)
    }

    /** The [ThreadPoolExecutor] used for executing queries. */
    private val executor = ThreadPoolExecutor(
            config.coreThreads,
            config.maxThreads,
            config.keepAliveMs,
            TimeUnit.MILLISECONDS,
            ArrayBlockingQueue(config.queueSize)
    )

    /** The [ExecutorCoroutineDispatcher] used for executing queries. */
    private val dispatcher = this.executor.asCoroutineDispatcher()

    /** Set of [ExecutionContext]s that are currently PENDING or RUNNING. */
    val contexts = ConcurrentHashMap<UUID, ExecutionContext>()

    /**
     * A concrete [ExecutionContext] used for executing a query.
     *
     * @author Ralph Gasser
     * @version 1.0.1
     */
    inner class ExecutionContext {

        /** The [UUID] that identifies this [ExecutionContext]. */
        val uuid: UUID = UUID.randomUUID()

        /** The [ExecutionStatus] of this [ExecutionContext]. */
        @Volatile
        var state: ExecutionStatus = ExecutionStatus.CREATED
            private set

        /** List of [SinkOperator]s that should be executed in this context. */
        private val operators: List<SinkOperator> = mutableListOf()

        /** Map of all [Entity.Tx] that have been created as part of this [ExecutionContext]. */
        private val transactions: MutableMap<Entity, Entity.Tx> = mutableMapOf()

        /** The maximum amount of parallelism allowed by this [ExecutionEngine] instance. */
        val maxThreads
            get() = this@ExecutionEngine.executor.maximumPoolSize

        /** The number of [Thread]s currently available. This is an estimate and may change very quickly. */
        val availableThreads
            get() = this@ExecutionEngine.executor.maximumPoolSize - this@ExecutionEngine.executor.activeCount

        val coroutineDispatcher
            get() = this@ExecutionEngine.dispatcher

        init {
            this@ExecutionEngine.contexts[this.uuid] = this
        }

        /**
         * Adds the given [SinkOperator] to the list of [SinkOperator]s that should be executed by this [ExecutionContext].
         *
         * @param operator The [SinkOperator] to add.
         */
        fun addOperator(operator: SinkOperator) {
            if (this.operators.contains(operator)) {
                throw IllegalArgumentException("Operator $operator cannot be added to list of operators because that operator is already part of that list.")
            }
            (this.operators as MutableList).add(operator)
        }

        /**
         * Requests a new [Entity.Tx] for the given [Entity].
         *
         * Calling this method will cause the [Entity.Tx] to be registered and opened. If
         * multiple operators request an [Entity.Tx] for the same [Entity], only one [Entity.Tx]
         * will be created. Furthermore, this method takes care of upgrading existing [readonly]
         * [Entity.Tx] to writeable [Entity.Tx] if necessary.
         *
         * @param entity [Entity] to request the [Entity.Tx] for.
         * @param readonly Whether the new [Entity.Tx] should be readonly.
         *
         * @return [Entity.Tx]
         */
        fun prepareTransaction(entity: Entity, readonly: Boolean) {
            if (!this.transactions.containsKey(entity)) {
                this.transactions[entity] = entity.Tx(readonly = readonly, tid = this.uuid)
            } else if (!readonly && this.transactions[entity]!!.readonly) {
                this.transactions[entity]!!.close() /* Upgrade transaction. */
                this.transactions[entity] = entity.Tx(readonly = readonly, tid = this.uuid)
            }
        }

        /**
         * Returns the [Entity.Tx] for the provided [Entity]. If such an [Entity.Tx] doesn't
         * exist, an [ExecutionException] is thrown.
         *
         * @param entity [Entity] to return the [Entity.Tx] for.
         * @return entity [Entity.Tx]
         */
        fun getTx(entity: Entity) = this.transactions[entity] ?: throw ExecutionException("")

        /**
         * Executes this [ExecutionContext] in the [dispatcher] of the enclosing [ExecutionEngine].
         */
        @Synchronized
        fun execute() {
            check(this.state == ExecutionStatus.CREATED) { "Cannot schedule ExecutionContext ${this.uuid} because it is in state ${this.state}." }

            /* Update state and execute. */
            this.state = ExecutionStatus.RUNNING
            runBlocking(this@ExecutionEngine.dispatcher) {
                for (operator in this@ExecutionContext.operators) {
                    /* Open operators. */
                    operator.open()

                    /* Execute flow. */
                    try {
                        operator.toFlow(this).collect()
                    } catch (e: OperatorExecutionException) {
                        throw e
                    } catch (e: Throwable) {
                        LOGGER.error("Unhandled exception during query execution: ${e.message}")
                        throw ExecutionException("Unhandled exception during query execution (${e.javaClass.simpleName})")
                    } finally {
                        /* Close all transactions. */
                        this@ExecutionContext.transactions.forEach { (_, tx) ->
                            tx.close()
                        }

                        /* Close operators. */
                        operator.close()
                    }
                }
            }

            /* Remove this ExecutionContext. */
            this.state = ExecutionStatus.COMPLETED
            this@ExecutionEngine.contexts.remove(this.uuid)
        }
    }
}