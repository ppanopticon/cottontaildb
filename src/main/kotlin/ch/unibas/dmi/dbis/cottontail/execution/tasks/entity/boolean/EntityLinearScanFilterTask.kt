package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.boolean

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.query
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset

import com.github.dexecutor.core.task.Task

/**
 * A [Task] that executes a full table boolean on a defined [Entity] using a [BooleanPredicate]
 * Only returns [Record][ch.unibas.dmi.dbis.cottontail.model.basics.Record]s that match the provided [BooleanPredicate].
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class EntityLinearScanFilterTask(private val entity: Entity, private val predicate: BooleanPredicate) : ExecutionTask("EntityLinearScanFilterTask[${entity.fqn}][$predicate]") {

    /** The cost of this [EntityLinearScanFilterTask] depends on whether or not an [Index] can be employed. */
    override val cost = (this.entity.statistics.columns * this.predicate.cost).toFloat()

    /**
     * Executes this [EntityLinearScanFilterTask]
     */
    override fun execute(): Recordset = this.entity.Tx(readonly = true, columns = this.predicate.columns.toTypedArray()).query {
        it.filter(this.predicate)
    } ?: Recordset(this.predicate.columns.toTypedArray())
}