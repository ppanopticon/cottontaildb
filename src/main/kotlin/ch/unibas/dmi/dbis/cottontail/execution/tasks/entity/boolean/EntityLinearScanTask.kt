package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.boolean

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.query
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import com.github.dexecutor.core.task.Task

/**
 * A [Task] that executes a full table boolean on a defined [Entity] and returns all the defined values.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class EntityLinearScanTask(private val entity: Entity, private val columns: Array<ColumnDef<*>>) : ExecutionTask("EntityLinearScanTask[${entity.fqn}][${columns.map { it.name }.joinToString(",")}]") {

    /** The cost of this [EntityLinearScanTask] is constant */
    override val cost = (entity.statistics.columns * columns.size).toFloat()

    /**
     * Executes this [EntityLinearScanTask]
     */
    override fun execute(): Recordset = this.entity.Tx(readonly = true, columns = this.columns).query {
        it.readAll()
    } ?: Recordset(columns)
}