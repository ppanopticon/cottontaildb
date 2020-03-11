package ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection

import ch.unibas.dmi.dbis.cottontail.database.queries.Projection
import ch.unibas.dmi.dbis.cottontail.database.queries.ProjectionType
import ch.unibas.dmi.dbis.cottontail.execution.cost.Costs
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.*
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name

import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException

/**
 * A [Task] used during query execution. It takes a single [Recordset] and determines the sum of a specific [ColumnDef]. It thereby creates a 1x1 [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class RecordsetSumProjectionTask(val projection: Projection, estimatedRows: Int = 1000): ExecutionTask("RecordsetSumProjectionTask") {

    /** The cost of this [RecordsetMaxProjectionTask] depends on the estimated size of the input. */
    override val cost = estimatedRows * Costs.MEMORY_ACCESS_READ

    init {
        assert(projection.type == ProjectionType.SUM)
    }

    /**
     * Executes this [RecordsetCountProjectionTask]
     */
    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first() ?: throw TaskExecutionException("SUM projection could not be executed because parent task has failed.")

        /* Calculate sum(). */
        val column = projection.columns.first()
        val resultsColumn = ColumnDef.withAttributes(this.projection.fields[column.name] ?: Name("max(${column.name})"), "DOUBLE")
        var sum = 0.0
        val results = Recordset(arrayOf(resultsColumn))
        parent.forEach {
            when (val value = it[column]) {
                is ByteValue -> sum += value.value
                is ShortValue -> sum += value.value
                is IntValue -> sum += value.value
                is LongValue -> sum += value.value
                is FloatValue -> sum += value.value
                is DoubleValue -> sum += value.value
                else -> {}
            }
        }
        results.addRowUnsafe(arrayOf(DoubleValue(sum)))
        return results
    }
}