package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.projection

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.query
import ch.unibas.dmi.dbis.cottontail.execution.cost.Costs
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.DoubleValue
import com.github.dexecutor.core.task.Task

/**
 * A [Task] used during query execution. It takes a single [Entity] and determines the minimum value of a specific [ColumnDef]. It thereby creates a 1x1 [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class EntityMinProjectionTask(val entity: Entity, val column: ColumnDef<*>, val alias: String? = null): ExecutionTask("EntityMinProjectionTask[${entity.name}]") {

    /** The cost of this [EntityExistsProjectionTask] is constant */
    override val cost = this.entity.statistics.rows * Costs.DISK_ACCESS_READ

    /**
     * Executes this [EntityExistsProjectionTask]
     */
    override fun execute(): Recordset {
        assertNullaryInput()

        val resultsColumn = ColumnDef.withAttributes(this.alias ?: "min(${this.column.name})", "DOUBLE")

        return this.entity.Tx(true, columns = arrayOf(this.column)).query {
            var min = Double.MAX_VALUE
            val recordset = Recordset(arrayOf(resultsColumn))
            it.forEach {
                when (val value = it[column]?.value) {
                    is Byte -> min = Math.min(min, value.toDouble())
                    is Short -> min = Math.min(min, value.toDouble())
                    is Int -> min = Math.min(min, value.toDouble())
                    is Long -> min = Math.min(min, value.toDouble())
                    is Float -> min = Math.min(min, value.toDouble())
                    is Double -> min = Math.min(min, value)
                    else -> {}
                }
            }
            recordset.addRowUnsafe(arrayOf(DoubleValue(min)))
            recordset
        }!!
    }
}