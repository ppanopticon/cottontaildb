package ch.unibas.dmi.dbis.cottontail.execution

import ch.unibas.dmi.dbis.cottontail.execution.tasks.ExecutionPlanException
import ch.unibas.dmi.dbis.cottontail.execution.tasks.ExecutionPlanSetupException
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionStage
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset

import com.github.dexecutor.core.DefaultDexecutor
import com.github.dexecutor.core.DexecutorConfig
import com.github.dexecutor.core.ExecutionConfig
import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskProvider

import org.slf4j.LoggerFactory
import java.util.*

import java.util.concurrent.ExecutorService

import kotlin.collections.HashMap


/**
 * A configurable execution plan for Cottontail DB queries. This class can be used to specify exactly,
 * how a query should be executed. The execution plan can be configured, by adding different,
 * interdependent [ExecutionTask]s to the [ExecutionPlan]
 *
 * @see ExecutionTask
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class ExecutionPlan(executor: ExecutorService) {

    /** The internal ID of this [ExecutionPlan]. */
    val id: UUID = UUID.randomUUID()

    /** The timestamp at which this [ExecutionPlan] was executed. */
    val timestamp = System.currentTimeMillis()

    /** The internal instance of [ExecutionPlanTaskProvider] that provides this [ExecutionPlan] with [ExecutionTask] instances. */
    private val provider: ExecutionPlanTaskProvider = this.ExecutionPlanTaskProvider()

    /** The [DexecutorConfig] used to setup the DAG execution. */
    private val config = DexecutorConfig<String, Recordset>(executor, this.provider)

    /** Logger used for logging the output of the final stage. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(FinalStageExecutionTask::class.java)
    }

    /**
     * Adds a new [ExecutionTask] to this [ExecutionPlan].
     *
     * @param task The [ExecutionTask] to add to the plan.
     * @param dependsOn The ID's of the [ExecutionTask]s the new [ExecutionTask] depends on. If none are given, the [ExecutionTask] is independent.
     * @return ID of the [ExecutionTask] that was just added.
     */
    fun addTask(task: ExecutionTask, vararg dependsOn: String): String {
        this.provider.addTask(task)
        if (dependsOn.isNotEmpty()) {
            for (id in dependsOn) {
                this.config.dexecutorState.addDependency(id, task.id)
            }
        } else {
            this.config.dexecutorState.addIndependent(task.id)
        }
        return task.id
    }

    /**
     * Adds a new [ExecutionStage] to this [ExecutionPlan].
     *
     * @param stage The [ExecutionStage] to add to the plan.
     * @param dependsOn The ID's of the [ExecutionTask]s the new [ExecutionStage] depends on. If none are given, the [ExecutionStage] is independent.
     * @return ID of last [ExecutionTask] in the [ExecutionStage] that was just added.
     */
    fun addStage(stage: ExecutionStage, vararg dependsOn: String): String {
        for (task in stage.tasks) {
            this.provider.addTask(task.key)
            if (task.value.isEmpty()) {
                if (dependsOn.isEmpty()) {
                    this.config.dexecutorState.addIndependent(task.key.id)
                } else {
                    for (dependency in dependsOn) {
                        this.config.dexecutorState.addDependency(dependency, task.key.id)
                    }
                }
            } else {
                for (dependency in task.value) {
                    this.config.dexecutorState.addDependency(task.key.id, dependency)
                }
            }
        }
        return stage.output!!
    }

    /**
     * Executes this [ExecutionPlan] and returns the results.
     *
     * @return The resulting [Recordset]
     * @throws ExecutionPlanException If any error occurred during execution.
     */
    fun execute(): Recordset {
        /* Prepare a final stage (it will hold the results of the execution plan). */
        val finalStage = this.FinalStageExecutionTask()
        if (!this.provider.hasTask(finalStage)) {
            this.provider.addTask(finalStage)
        }
        this.config.dexecutorState.addAsDependentOnAllLeafNodes(finalStage.id)

        /* Execute plan and check for errors. */
        val errors = DefaultDexecutor(this.config).execute(ExecutionConfig.TERMINATING)
        if (errors.hasAnyResult()) {
            throw ExecutionPlanException(this, errors.first.message)
        }

        /* If no error occurred, then check + return results of final stage. */
        if (!finalStage.parentResults.hasAnyResult() || !finalStage.parentResults.first.isSuccess) {
            throw ExecutionPlanException(this, "Final stage does not contain any results.")
        }
        return finalStage.parentResults.first.result
    }

    /**
     * An additional, internal stage used by [ExecutionPlan] to access the results of the query execution.
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    private inner class FinalStageExecutionTask: ExecutionTask("FinalStage[${this@ExecutionPlan.id}]") {
        override val cost = 0.0f
        override fun execute(): Recordset {
            if (!this.parentResults.hasAnyResult()) {
                throw ExecutionPlanException(this@ExecutionPlan, "Final stage in execution plan did not produce any results.")
            }

            if (this.parentResults.all.size > 1) {
                throw ExecutionPlanException(this@ExecutionPlan, "Invalid execution plan! Final stage in execution plan did produce more than one result set.")
            }

            /* Log successful execution. */
            val output = this.first() ?: throw ExecutionPlanException(this@ExecutionPlan, "Final stage in execution plan failed!")
            LOGGER.debug("Execution plan ${this@ExecutionPlan.id} has been run successfully! It produced a ${output.rowCount} x ${output.columnCount} record set.")
            LOGGER.debug("The column names are: ${output.columns.joinToString { it.name.toString() }}")
            return output
        }
    }

    /**
     * The [TaskProvider] instance used by [ExecutionPlan]. It is backed by a HashMap.
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    private inner class ExecutionPlanTaskProvider: TaskProvider<String, Recordset> {

        /** List of [Task]s that will be executed in this [ExecutionPlan]. */
        private val tasks: HashMap<String, ExecutionTask> = HashMap()

        /**
         * Returns the task for the given ID.
         *
         * @param id The ID of the requested task.
         * @return The [Task] for
         */
        override fun provideTask(id: String?): ExecutionTask = tasks[id] ?: throw ExecutionPlanException(this@ExecutionPlan, "The task provider failed to provide a task for ID '$id'.")

        /**
         * Adds a [Task] task to this [ExecutionPlan]. By doing so, the [Task] will
         * receive a unique ID, which will be returned by the method.
         *
         * @param task The [Task] that should be added.
         * @return The ID of the task in the [ExecutionPlan]
         */
        fun addTask(task: ExecutionTask) {
            if (!this.tasks.containsKey(task.id)) {
                this.tasks[task.id] = task
            } else {
                throw ExecutionPlanSetupException(this@ExecutionPlan, "A task with ID ${task.id} already exists for execution plan.")
            }
        }

        /**
         * Checks if the provided [ExecutionTask] is already contained in this [ExecutionPlan].
         *
         * @param task The task to check.
         */
        fun hasTask(task: ExecutionTask): Boolean = this.tasks.containsKey(task.id)
    }
}

