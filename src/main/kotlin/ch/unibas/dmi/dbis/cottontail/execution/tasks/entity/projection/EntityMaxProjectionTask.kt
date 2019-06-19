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
 * A [Task] used during query execution. It takes a single [Entity] and determines the maximum value of a specific [ColumnDef]. It thereby creates a 1x1 [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class EntityMaxProjectionTask(val entity: Entity, val column: ColumnDef<*>, val alias: String? = null): ExecutionTask("EntityMaxProjectionTask[${entity.name}]") {

    /** The cost of this [EntityExistsProjectionTask] is constant */
    override val cost = this.entity.statistics.rows * Costs.DISK_ACCESS_READ

    /**
     * Executes this [EntityExistsProjectionTask]
     */
    override fun execute(): Recordset {
        assertNullaryInput()

        val resultsColumn = ColumnDef.withAttributes(this.alias ?: "max(${this.column.name})", "DOUBLE")

        return this.entity.Tx(true, columns = arrayOf(this.column)).query {
            var max = Double.MIN_VALUE
            val recordset = Recordset(arrayOf(resultsColumn))
            it.forEach {
                when (val value = it[column]?.value) {
                    is Byte -> max = Math.max(max, value.toDouble())
                    is Short -> max = Math.max(max, value.toDouble())
                    is Int -> max = Math.max(max, value.toDouble())
                    is Long -> max = Math.max(max, value.toDouble())
                    is Float -> max = Math.max(max, value.toDouble())
                    is Double -> max = Math.max(max, value)
                    else -> {}
                }
            }
            recordset.addRowUnsafe(arrayOf(DoubleValue(max)))
            recordset
        }!!
    }
}