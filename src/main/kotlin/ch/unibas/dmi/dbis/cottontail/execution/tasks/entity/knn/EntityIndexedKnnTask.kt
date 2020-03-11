package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnType
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.query
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.queries.KnnPredicate
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.boolean.EntityLinearScanFilterTask
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.types.VectorValue

import com.github.dexecutor.core.task.Task

/**
 * A [Task] that executes a index based kNN on the specified [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class EntityIndexedKnnTask<T: VectorValue<*>>(val entity: Entity, val knnPredicate: KnnPredicate<T>, indexHint: Index) : ExecutionTask("EntityIndexedKnnTask[${entity.fqn}][${knnPredicate.column.name}][${knnPredicate.distance::class.simpleName}][${knnPredicate.k}][q=${knnPredicate.query.hashCode()}]") {
    /** The cost of this [EntityLinearScanFilterTask] depends on whether or not an [Index] can be employed. */
    override val cost = indexHint.cost(this.knnPredicate)

    /** List of the [ColumnDef] this instance of [ParallelEntityScanKnnTask] produces. */
    private val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef(this.entity.fqn.append("distance"), ColumnType.forName("DOUBLE")))

    /** The type of the [Index] that should be used.*/
    private val type = indexHint.type

    override fun execute(): Recordset = this.entity.Tx(readonly = true, columns = knnPredicate.columns.toTypedArray()).query { tx ->
        val index = tx.indexes(this.knnPredicate.columns.toTypedArray(), this.type).first()
        index.filter(this.knnPredicate)
    } ?: Recordset(this.produces, capacity = 0)
}