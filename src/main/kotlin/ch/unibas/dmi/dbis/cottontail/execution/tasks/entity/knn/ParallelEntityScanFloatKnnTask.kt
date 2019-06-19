package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.KnnPredicate
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.math.knn.ComparablePair
import ch.unibas.dmi.dbis.cottontail.math.knn.HeapSelect
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.type.TypeFactory
import ch.unibas.dmi.dbis.cottontail.model.values.DoubleValue
import com.github.dexecutor.core.task.Task

/**
 * A [Task] that executes a parallel boolean kNN on a float [Column] of the specified [Entity].
 * Parallelism is achieved through the use of co-routines.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
internal class ParallelEntityScanFloatKnnTask(val entity: Entity, val knn: KnnPredicate<FloatArray>, val predicate: BooleanPredicate? = null, val parallelism: Short = 2) : ExecutionTask("ParallelEntityScanDoubleKnnTask[${entity.fqn}][${knn.column.name}][${knn.distance::class.simpleName}][${knn.k}][q=${knn.query.hashCode()}]") {

    /** Set containing the kNN values (per query vector). */
    private val knnSet = knn.query.map { HeapSelect<ComparablePair<Long,Double>>(this.knn.k) }

    /** List of the [ColumnDef] this instance of [ParallelEntityScanDoubleKnnTask] produces. */
    private val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef("${entity.fqn}.distance", TypeFactory.forName("DOUBLE")))

    /** The cost of this [ParallelEntityScanDoubleKnnTask] is constant */
    override val cost = entity.statistics.columns * (knn.operations + (predicate?.operations ?: 0)).toFloat() / parallelism

    /**
     * Executes this [ParallelEntityScanDoubleKnnTask]
     */
    override fun execute(): Recordset {
        /* Extract the necessary data. */
        val queries = this.knn.query.map {array -> FloatArray(array.size) { array[it].toFloat() } }
        val weights = this.knn.weights?.map { array -> FloatArray(array.size) { array[it].toFloat() } }
        val columns = arrayOf<ColumnDef<*>>(this.knn.column).plus(predicate?.columns?.toTypedArray() ?: emptyArray())

        /* Execute kNN lookup. */
        this.entity.Tx(readonly = true, columns = columns).begin { tx ->
            tx.forEach(this.parallelism) {
                if (this.predicate == null || this.predicate.matches(it)) {
                    val value = it[this.knn.column]
                    if (value != null) {
                        queries.forEachIndexed { i, query ->
                            if (weights != null) {
                                this.knnSet[i].add(ComparablePair(it.tupleId, this.knn.distance(query, value.value, weights[i])))
                            } else {
                                this.knnSet[i].add(ComparablePair(it.tupleId, this.knn.distance(query, value.value)))
                            }
                        }
                    }
                }
            }
            true
        }

        /* Generate dataset and return it. */
        val dataset = Recordset(this.produces)
        for (knn in this.knnSet) {
            for (i in 0 until knn.size) {
                dataset.addRowUnsafe(knn[i].first, arrayOf(DoubleValue(knn[i].second)))
            }
        }
        return dataset
    }
}