package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.fetch

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection.RecordsetCountProjectionTask
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException

/**
 * A [Task] used during query execution. It takes a [Recordset] as input, fetches the desired [Column][ch.unibas.dmi.dbis.cottontail.database.column.Column]s
 * from the specified [Entity] and appends them to the original [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class EntityFetchColumnsTask (val entity: Entity, val columns: Array<ColumnDef<*>>): ExecutionTask("EntityFetchColumnsTask[${entity.fqn}") {
    /** The cost of this [RecordsetCountProjectionTask] is constant */
    override val cost = (this.entity.statistics.rows * columns.size) * 1e-6f

    /**
     * Executes this [RecordsetCountProjectionTask]
     */
    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first() ?: throw TaskExecutionException("Projection could not be executed because parent task has failed.")

        /* Determine the columns to fetch and all columns. */
        val all = (parent.columns + columns).distinct().toTypedArray()
        val fetch = this.columns.filter { !parent.columns.contains(it) }.toTypedArray()

        /* Prepare new Recordset and fill it with data. */
        if (fetch.isNotEmpty()) {
            val recordset = Recordset(all, capacity = parent.rowCount)
            this.entity.Tx(readonly = true, columns = fetch).begin {tx ->
                parent.forEach {rec ->
                    val read = tx.read(rec.tupleId)
                    val values: Array<Value?> = all.map {
                        if (read.has(it)) {
                            read[it]
                        } else {
                            rec[it]
                        }
                    }.toTypedArray()
                    recordset.addRowUnsafe(rec.tupleId, values)
                }
                true
            }

            return recordset
        } else {
            return parent
        }
    }
}