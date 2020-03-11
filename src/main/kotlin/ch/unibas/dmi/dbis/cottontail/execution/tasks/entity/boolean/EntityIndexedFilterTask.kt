package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.boolean

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.query
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset

/**
 * A [Task][com.github.dexecutor.core.task.Task] that executes data access through an index on a defined
 * [Entity] using a [BooleanPredicate]. Only returns [Record][ch.unibas.dmi.dbis.cottontail.model.basics.Record]s
 * that match the provided [BooleanPredicate].
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class EntityIndexedFilterTask(private val entity: Entity, private val predicate: BooleanPredicate, indexHint: Index) : ExecutionTask("EntityIndexedFilterTask[${entity.fqn}][$predicate]") {
    /** The cost of this [EntityLinearScanFilterTask] depends on whether or not an [Index] can be employed. */
    override val cost = indexHint.cost(this.predicate)

    /** The type of the [Index] that should be used.*/
    private val type = indexHint.type

    /**
     * Executes this [EntityIndexedFilterTask]
     */
    override fun execute(): Recordset = this.entity.Tx(readonly = true, columns = emptyArray()).query {tx ->
        val index = tx.indexes(this.predicate.columns.toTypedArray(), this.type).first()
        val dataset = Recordset(index.produces)
        index.forEach(this.predicate) {
            dataset.addRowUnsafe(it.tupleId, it.values)
        }
        dataset
    } ?: Recordset(this.predicate.columns.toTypedArray(), capacity = 0)
}