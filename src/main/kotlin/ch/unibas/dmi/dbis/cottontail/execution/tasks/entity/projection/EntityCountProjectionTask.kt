package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.projection

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.query
import ch.unibas.dmi.dbis.cottontail.execution.cost.Costs
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.LongValue
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import com.github.dexecutor.core.task.Task

/**
 * A [Task] used during query execution. It takes a single [Entity] as input, counts the number of rows. It thereby creates a 1x1 [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0.2
 */
class EntityCountProjectionTask (val entity: Entity): ExecutionTask("EntityCountProjectionTask[${entity.fqn}") {

    /** The cost of this [EntityCountProjectionTask] is constant */
    override val cost = Costs.DISK_ACCESS_READ

    /**
     * Executes this [EntityCountProjectionTask]
     */
    override fun execute(): Recordset {
        assertNullaryInput()

        val column = arrayOf(ColumnDef.withAttributes(Name("count(${entity.fqn})"), "LONG"))
        return this.entity.Tx(true).query {
            val recordset = Recordset(column, capacity = 1)
            recordset.addRowUnsafe(arrayOf(LongValue(it.count())))
            recordset
        }!!
    }
}