package ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection

import ch.unibas.dmi.dbis.cottontail.execution.cost.Costs
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask

import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.IntValue
import ch.unibas.dmi.dbis.cottontail.model.values.LongValue
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name

import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException

/**
 * A [Task] used during query execution. It takes a single [Recordset] as input, counts the number of of rows and returns it as [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class RecordsetCountProjectionTask (): ExecutionTask("RecordsetCountProjectionTask") {

    /** The cost of this [RecordsetCountProjectionTask] is constant. */
    override val cost = Costs.MEMORY_ACCESS_READ

    /**
     * Executes this [RecordsetCountProjectionTask]
     */
    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first() ?: throw TaskExecutionException("COUNT projection could not be executed because parent task has failed.")

        /* Create new Recordset with new columns. */
        val recordset = Recordset(arrayOf(ColumnDef.withAttributes(Name("count(*)"), "LONG")))
        recordset.addRowUnsafe(arrayOf(LongValue(parent.rowCount)))
        return recordset
    }
}