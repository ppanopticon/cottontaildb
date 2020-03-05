package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.projection

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.query
import ch.unibas.dmi.dbis.cottontail.execution.cost.Costs
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.*
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import com.github.dexecutor.core.task.Task
import java.lang.Double.min

/**
 * A [Task] used during query execution. It takes a single [Entity] and determines the minimum value of a specific [ColumnDef]. It thereby creates a 1x1 [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0.2
 */
class EntityMinProjectionTask(val entity: Entity, val column: ColumnDef<*>, val alias: String? = null): ExecutionTask("EntityMinProjectionTask[${entity.name}]") {

    /** The cost of this [EntityExistsProjectionTask] is constant */
    override val cost = this.entity.statistics.rows * Costs.DISK_ACCESS_READ

    /**
     * Executes this [EntityExistsProjectionTask]
     */
    override fun execute(): Recordset {
        assertNullaryInput()

        val resultsColumn = ColumnDef.withAttributes(Name(this.alias ?: "min(${this.column.name})"), "DOUBLE")

        return this.entity.Tx(true, columns = arrayOf(this.column)).query { tx ->
            var min = Double.MAX_VALUE
            val recordset = Recordset(arrayOf(resultsColumn), capacity = 1)
            tx.forEach {
                when (val value = it[column]) {
                    is ByteValue -> min = min(min, value.value.toDouble())
                    is ShortValue -> min = min(min, value.value.toDouble())
                    is IntValue -> min = min(min, value.value.toDouble())
                    is LongValue -> min = min(min, value.value.toDouble())
                    is FloatValue -> min = min(min, value.value.toDouble())
                    is DoubleValue -> min = min(min, value.value)
                    else -> {}
                }
            }
            recordset.addRowUnsafe(arrayOf(DoubleValue(min)))
            recordset
        }!!
    }
}