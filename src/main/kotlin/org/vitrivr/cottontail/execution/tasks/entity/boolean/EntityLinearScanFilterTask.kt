package org.vitrivr.cottontail.execution.tasks.entity.boolean

import com.github.dexecutor.core.task.Task
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.query
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.BooleanPredicate
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.recordset.Recordset

/**
 * A [Task] that executes a full table boolean on a defined [Entity] using a [BooleanPredicate]
 * Only returns [Record][org.vitrivr.cottontail.model.basics.Record]s that match the provided [BooleanPredicate].
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