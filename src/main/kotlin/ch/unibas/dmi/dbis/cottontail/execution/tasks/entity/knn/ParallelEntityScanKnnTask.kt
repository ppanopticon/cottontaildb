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
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

/**
 * A [Task] that executes a parallel boolean kNN on a [Column][ch.unibas.dmi.dbis.cottontail.database.column.Column]
 * of the specified [Entity]. Parallelism is achieved through the use of co-routines.
 *
 * @author Ralph Gasser
 * @version 1.1.1
 */
class ParallelEntityScanKnnTask<T: VectorValue<*>>(val entity: Entity, val knn: KnnPredicate<T>, val predicate: BooleanPredicate? = null, val parallelism: Short = 2) : ExecutionTask("ParallelEntityScanDoubleKnnTask[${entity.fqn}][${knn.column.name}][${knn.distance::class.simpleName}][${knn.k}][q=${knn.query.hashCode()}]") {

    /** Set containing the kNN values. */
    private val knnSet = knn.query.map { HeapSelect<ComparablePair<Long,Double>>(this.knn.k) }

    /** List of the [ColumnDef] this instance of [ParallelEntityScanKnnTask] produces. */
    private val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef(this.entity.fqn.append("distance"), ColumnType.forName("DOUBLE")))

    /** The cost of this [ParallelEntityScanKnnTask] is constant */
    override val cost = (entity.statistics.columns * this.knn.cost + (predicate?.cost ?: 0.0) / parallelism).toFloat()

    /**
     * Executes this [ParallelEntityScanKnnTask]
     */
    override fun execute(): Recordset = this.entity.Tx(readonly = true, columns = arrayOf<ColumnDef<*>>(this.knn.column).plus(this.predicate?.columns?.toTypedArray() ?: emptyArray())).query { tx ->
        /* Extract the necessary data. */
        val columns = arrayOf<ColumnDef<*>>(this.knn.column).plus(predicate?.columns?.toTypedArray() ?: emptyArray())

        /* Execute kNN lookup. */
        val maxTupleId = this.entity.statistics.maxTupleId
        val blocksize = maxTupleId / this.parallelism

        val action: (Record) -> Unit = if (this@ParallelEntityScanKnnTask.knn.weights != null) {
            {
                val v = it[this@ParallelEntityScanKnnTask.knn.column]
                if (v != null) {
                    this@ParallelEntityScanKnnTask.knn.query.forEachIndexed { i, q ->
                        this@ParallelEntityScanKnnTask.knnSet[i].add(ComparablePair(it.tupleId, this@ParallelEntityScanKnnTask.knn.distance(q, v, this@ParallelEntityScanKnnTask.knn.weights[i])))
                    }
                }
            }
        } else {
            {
                val v = it[this@ParallelEntityScanKnnTask.knn.column]
                if (v != null) {
                    this@ParallelEntityScanKnnTask.knn.query.forEachIndexed { i, q ->
                        this@ParallelEntityScanKnnTask.knnSet[i].add(ComparablePair(it.tupleId, this@ParallelEntityScanKnnTask.knn.distance(q, v)))
                    }
                }
            }
        }

        /* Execute kNN lookup. */
        runBlocking {
            val jobs = Array(this@ParallelEntityScanKnnTask.parallelism.toInt()) { j ->
                GlobalScope.launch {
                    if (this@ParallelEntityScanKnnTask.predicate != null) {
                        tx.forEach(blocksize * j + 1L, blocksize * j + blocksize, this@ParallelEntityScanKnnTask.predicate, action)
                    } else {
                        tx.forEach(blocksize * j + 1L, blocksize * j + blocksize, action)
                    }
                }
            }
            jobs.forEach { it.join() }
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