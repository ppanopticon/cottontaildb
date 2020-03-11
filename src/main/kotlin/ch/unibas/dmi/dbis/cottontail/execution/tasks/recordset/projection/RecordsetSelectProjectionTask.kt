package ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.queries.Projection
import ch.unibas.dmi.dbis.cottontail.database.queries.ProjectionType
import ch.unibas.dmi.dbis.cottontail.execution.cost.Costs

import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask

import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.utilities.name.Match
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name

import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException

/**
 * A [Task] used during query execution. It takes a single [Recordset] as input and fetches the specified [Column][ch.unibas.dmi.dbis.cottontail.database.column.Column]
 * values from the specified [Entity] in order to addRow these values to the resulting [Recordset].
 *
 * A [Recordset] that undergoes this task will grow in terms of columns it contains. However, it should not affect the number of rows.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class RecordsetSelectProjectionTask (val projection: Projection, estimatedRows: Int = 1000, estimatedColumns: Int = 5): ExecutionTask("RecordsetSelectProjectionTask") {

    /** Cost estimate for this [RecordsetSelectProjectionTask] depends on the input size. */
    override val cost = estimatedRows * estimatedColumns * Costs.MEMORY_ACCESS_READ

    init {
        assert(projection.type == ProjectionType.SELECT)
    }

    /**
     * Executes this [RecordsetSelectProjectionTask]
     */
    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first() ?: throw TaskExecutionException("SELECT projection could not be executed because parent task has failed.")

        /* Find longest, common prefix of all projection sub-clauses. */
        val longestCommonPrefix = Name.findLongestCommonPrefix(this.projection.fields.keys)

        /* Determine columns to rename. */
        val dropIndex = mutableListOf<Int>()
        val renameIndex = mutableListOf<Pair<Int,Name>>()
        parent.columns.forEachIndexed { i, c ->
            var match = false
            this.projection.fields.forEach { (k, v) ->
                val m = k.match(c.name)
                if (m != Match.NO_MATCH) {
                    match = true
                    if (v != null) {
                        renameIndex.add(Pair(i, v))
                    } else if (longestCommonPrefix != Name.COTTONTAIL_ROOT_NAME) {
                        renameIndex.add(Pair(i, c.name.normalize(longestCommonPrefix)))
                    }
                }
            }
            if (!match) dropIndex.add(i)
        }

        /* Create new Recordset with specified columns and return it . */
        return if (dropIndex.size > 0 && renameIndex.size > 0) {
            parent.renameColumnsWithIndex(renameIndex).dropColumnsWithIndex(dropIndex)
        } else if (renameIndex.size > 0) {
            parent.renameColumnsWithIndex(renameIndex)
        } else if (dropIndex.size > 0) {
            parent.dropColumnsWithIndex(dropIndex)
        } else {
            return parent
        }
    }
}