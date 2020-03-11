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
import java.lang.Double.max

/**
 * A [Task] used during query execution. It takes a single [Recordset] and determines the maximum value of a specific [ColumnDef]. It thereby creates a 1x1 [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class RecordsetMaxProjectionTask(val projection: Projection, estimatedRows: Int = 1000): ExecutionTask("RecordsetMaxProjectionTask") {

    /** The cost of this [RecordsetMaxProjectionTask]  depends on the estimated size of the input. */
    override val cost = estimatedRows * Costs.MEMORY_ACCESS_READ

    init {
        assert(projection.type == ProjectionType.MAX)
    }

    /**
     * Executes this [RecordsetCountProjectionTask]
     */
    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first() ?: throw TaskExecutionException("MAX projection could not be executed because parent task has failed.")

        /* Calculate max(). */
        val column = projection.columns.first()
        val resultsColumn = ColumnDef.withAttributes(this.projection.fields[column.name] ?: Name("max(${column.name})"), "DOUBLE")
        var max = Double.MIN_VALUE
        val results = Recordset(arrayOf(resultsColumn))
        parent.forEach {
            when (val value = it[column]) {
                is ByteValue -> max = max(max, value.value.toDouble())
                is ShortValue -> max = max(max, value.value.toDouble())
                is IntValue -> max = max(max, value.value.toDouble())
                is LongValue -> max = max(max, value.value.toDouble())
                is FloatValue -> max = max(max, value.value.toDouble())
                is DoubleValue -> max = max(max, value.value)
                else -> {}
            }
        }
        results.addRowUnsafe(arrayOf(DoubleValue(max)))
        return results
    }
}