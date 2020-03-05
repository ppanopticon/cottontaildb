package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnType
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.query
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.KnnPredicate
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.math.knn.ComparablePair
import ch.unibas.dmi.dbis.cottontail.math.knn.HeapSelect
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.DoubleValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.VectorValue

import com.github.dexecutor.core.task.Task

/**
 * A [Task] that executes a sequential kNN on a float [Column][ch.unibas.dmi.dbis.cottontail.database.column.Column] of the specified [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1.1
 */
class LinearEntityScanKnnTask<T: VectorValue<*>>(val entity: Entity, val knn: KnnPredicate<T>, val predicate: BooleanPredicate? = null) : ExecutionTask("LinearEntityScanKnnTask[${entity.fqn}][${knn.column.name}][${knn.distance::class.simpleName}][${knn.k}][q=${knn.query.hashCode()}]") {

    /** Set containing the kNN values. */
    private val knnSet = knn.query.map { HeapSelect<ComparablePair<Long,Double>>(this.knn.k) }

    /** List of the [ColumnDef] this instance of [LinearEntityScanKnnTask] produces. */
    private val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef(this.entity.fqn.append("distance"), ColumnType.forName("DOUBLE")))

    /** The cost of this [LinearEntityScanKnnTask] is constant */
    override val cost = (this.entity.statistics.columns * this.knn.cost.toFloat() + (predicate?.cost ?: 0.0)).toFloat()

    /**
     * Executes this [LinearEntityScanKnnTask]
     */
    override fun execute(): Recordset = this.entity.Tx(readonly = true, columns = arrayOf<ColumnDef<*>>(this.knn.column).plus(this.predicate?.columns?.toTypedArray() ?: emptyArray())).query { tx ->
        /* Extract the necessary data. */
        val action: (Record) -> Unit = if (this.knn.weights != null) {
            {
                val value = it[this.knn.column]
                if (value != null) {
                    this.knn.query.forEachIndexed { i, query ->
                        this.knnSet[i].add(ComparablePair(it.tupleId, this.knn.distance(query, value, this.knn.weights[i])))
                    }
                }
            }
        } else {
            {
                val value = it[this.knn.column]
                if (value != null) {
                    this.knn.query.forEachIndexed { i, query ->
                        this.knnSet[i].add(ComparablePair(it.tupleId, this.knn.distance(query, value)))
                    }
                }
            }
        }

        if (this.predicate != null) {
            tx.forEach(this.predicate, action)
        } else {
            tx.forEach(action)
        }

        /* Generate dataset and return it. */
        val dataset = Recordset(this.produces, capacity = (this.knnSet.size * this.knn.k).toLong())
        for (knn in this.knnSet) {
            for (i in 0 until knn.size) {
                dataset.addRowUnsafe(knn[i].first, arrayOf(DoubleValue(knn[i].second)))
            }
        }
        dataset
    } ?: Recordset(this.produces, capacity = 0)
}