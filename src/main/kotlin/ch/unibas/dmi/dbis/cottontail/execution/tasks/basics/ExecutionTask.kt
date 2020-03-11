package ch.unibas.dmi.dbis.cottontail.execution.tasks.basics

import ch.unibas.dmi.dbis.cottontail.execution.tasks.TaskExecutionException
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import com.github.dexecutor.core.task.Task
import java.util.*


/**
 * A single task usually executed as part of a query. Such as task generated a [Recordset] by  fetching or
 * transforming data. [ExecutionTask]s are usually part of an [ExecutionPlan][ch.unibas.dmi.dbis.cottontail.execution.ExecutionPlan].
 *
 * @see ch.unibas.dmi.dbis.cottontail.execution.ExecutionPlan
 * @see Recordset
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class ExecutionTask(name: String): Task<String, Recordset>() {

    /** Initializes this [ExecutionTask]'s ID. */
    init {
        this.id = "$name[${UUID.randomUUID()}]"
    }

    /** The estimated cost of executing this [ExecutionTask]. */
    abstract val cost: Float

    /**
     * Convenience method: Returns the first result, if it was successful
     *
     * @return Optional, first result.
     */
    protected fun first(): Recordset? = if (this.parentResults?.first!!.isSuccess) {this.parentResults?.first!!.result} else {null}

    /**
     * Convenience method: Returns the first result, if it was successful
     *
     * @return Optional, first result.
     */
    protected fun get(i: Int): Recordset? = this.parentResults.all[i]?.let {
        if (it.isSuccess) {
            it.result
        } else {
            null
        }
    }

    /**
     * Convenience method: Returns all results (regardless of being successful).
     *
     * @return List of all results.
     */
    protected fun all(): Collection<Recordset> = this.parentResults.all.map { it.result }

    /**
     * Convenience method: Returns all successful results.
     *
     * @return List of all successful results.
     */
    protected fun allSuccessful(): Collection<Recordset> = this.parentResults.all.filter {it.isSuccess} .map { it.result }

    /**
     * Asserts that the input provided by the parent [ExecutionTask] is unary (i.e. exactly one input that did not fail).
     *
     * @throws TaskExecutionException If parent task provided non-unary output.
     */
    protected fun assertUnaryInput() {
        if (!this.parentResults.hasAnyResult()) {
            throw TaskExecutionException(this, "Parent task did not provide any output but is expected to be unary. Please connect a valid parent task.")
        }

        if (this.parentResults.all.size != 1) {
            throw TaskExecutionException(this, "Parent task did provide more than one output but is expected to be unary. Please foresee a merging stage.")
        }
    }

    /**
     * Asserts that the input provided by the parent [ExecutionTask] is unary (i.e. exactly one input that did not fail).
     *
     * @throws TaskExecutionException If parent task provided non-unary output.
     */
    protected fun assertBinaryInput() {
        if (!this.parentResults.hasAnyResult()) {
            throw TaskExecutionException(this, "Parent task did not provide any output but is expected to be unary. Please connect a valid parent task.")
        }

        if (this.parentResults.all.size != 2) {
            throw TaskExecutionException(this, "Parent task did provide more or less than two outputs but is expected to be binary. Please foresee a merging stage.")
        }
    }

    /**
     * Asserts that the input provided by the parent [ExecutionTask] is nullary (i.e. that no parent task exists).
     */
    protected fun assertNullaryInput() {
        if (this.parentResults.hasAnyResult()) {
            throw TaskExecutionException(this, "Parent task did provide output but is expected to be nullary!")
        }
    }
}