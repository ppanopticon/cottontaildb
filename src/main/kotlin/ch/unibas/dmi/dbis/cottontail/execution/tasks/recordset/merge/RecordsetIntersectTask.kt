package ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.merge

import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException
import java.lang.IllegalArgumentException

/**
 * A [Task] used during query execution. It takes two [Recordset]s as input and creates a new [Recordset] containing the INTERSECTION of the two inputs. The INTERSECTION is
 * based on the tuple ID of the individual [Record][ch.unibas.dmi.dbis.cottontail.model.basics.Record]s.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class RecordsetIntersectTask (sizeEstimate: Int = 1000): ExecutionTask("RecordsetUnionTask") {
    /** The estimated cost of this [RecordsetIntersectTask] depends linearly on the size estimate. */
    override val cost = 0.0025f * sizeEstimate

    override fun execute(): Recordset {
        assertBinaryInput()

        val left = this.get(0) ?: throw TaskExecutionException("Recordset INTERSECT could not be executed because left recordset does not exists.")
        val right = this.get(1) ?: throw TaskExecutionException("Recordset INTERSECT could not be executed because right recordset does not exists.")

        try {
            return left.intersect(right)
        } catch (e: IllegalArgumentException) {
            throw TaskExecutionException("Recordset INTERSECT could not be executed because left recordset does equal right recordset.")
        }
    }
}