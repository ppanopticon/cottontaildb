package ch.unibas.dmi.dbis.cottontail.calcite.knn

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.math.knn.ComparablePair
import ch.unibas.dmi.dbis.cottontail.math.knn.HeapSelect
import ch.unibas.dmi.dbis.cottontail.math.knn.metrics.Distance
import ch.unibas.dmi.dbis.cottontail.model.values.Value
import org.apache.calcite.schema.impl.TableFunctionImpl
import kotlin.math.max
import kotlin.math.min

/**
 * This class represents an abstract implementation for a TABLE function, that can be used for kNN lookup. Subclasses of this class
 * can be used by Apache Calcite as a [TableFunctionImpl].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal abstract class AbstractScanningKnn {

    companion object {
        const val PARALLELISM_THRESHOLD = 250000
    }

    /**
     * Performs a kNN lookup with [DoubleArray]s by scanning an [Entity] using the given [Entity.Tx]. Parallelism is determined based on available CPU's and the estimated size of the dataset.
     *
     * @param tx The [Entity.Tx] to use for the table scan.
     * @param id_col The column that acts as ID for the resulting table. Can be unique but uniqueness is not a requirement!
     * @param vec_col The column that acts as vector for the distance calculation. Must be of type [DoubleArray]!
     * @param k The k in kNN, i.e. the maximum number of entries to retain.
     * @param distance The [Distance] function that should be used for calculation.
     * @param queries List of query vectors. Can contain more than one vector, each of which must be of type [DoubleArray].
     * @param weights The optional weights. If a list is provided, then the number of weights must be the same as the number of queries.
     *
     * @return A list of [HeapSelect] objects, one for each query vector.
     */
    protected fun doubleKnn(tx: Entity.Tx, id_col: ColumnDef<*>, vec_col: ColumnDef<DoubleArray>, k: Int, distance: Distance, queries: List<DoubleArray>, weights: List<DoubleArray>? = null) : List<HeapSelect<ComparablePair<Value<*>, Double>>>  {
        val knnSet = queries.map { HeapSelect<ComparablePair<Value<*>,Double>>(k) }
        val parallelism = determineParallelism(tx.count())
        if (weights != null) {
            tx.forEach(parallelism) {
                val value = it[vec_col]
                if (value != null) {
                    queries.forEachIndexed { i, query ->
                        knnSet[i].add(ComparablePair(it[id_col] as Value<*>, distance(query, value.value, weights[i])))
                    }
                }
            }
        } else {
            tx.forEach(parallelism) {
                val value = it[vec_col]
                if (value != null) {
                    queries.forEachIndexed { i, query ->
                        knnSet[i].add(ComparablePair(it[id_col] as Value<*>, distance(query, value.value)))
                    }
                }
            }
        }
        return knnSet
    }

    /**
     * Performs a kNN lookup with [FloatArray]s by scanning an [Entity] using the given [Entity.Tx]. Parallelism is determined based on available CPU's and the estimated size of the dataset.
     *
     * @param tx The [Entity.Tx] to use for the table scan.
     * @param id_col The column that acts as ID for the resulting table. Can be unique but uniqueness is not a requirement!
     * @param vec_col The column that acts as vector for the distance calculation. Must be of type [FloatArrayType]!
     * @param k The k in kNN, i.e. the maximum number of entries to retain.
     * @param distance The [Distance] function that should be used for calculation.
     * @param queries List of query vectors. Can contain more than one vector, each of which must be of type [FloatArray].
     * @param weights The optional weights. If a list is provided, then the number of weights must be the same as the number of queries.
     *
     * @return A list of [HeapSelect] objects, one for each query vector.
     */
    protected fun floatKnn(tx: Entity.Tx, id_col: ColumnDef<*>, vec_col: ColumnDef<FloatArray>, k: Int, distance: Distance, queries: List<FloatArray>, weights: List<FloatArray>? = null) : List<HeapSelect<ComparablePair<Value<*>, Double>>>  {
        val knnSet = queries.map { HeapSelect<ComparablePair<Value<*>,Double>>(k) }
        val parallelism = determineParallelism(tx.count())
        if (weights != null) {
            tx.forEach(parallelism) {
                val value = it[vec_col]
                if (value != null) {
                    queries.forEachIndexed { i, query ->
                        knnSet[i].add(ComparablePair(it[id_col] as Value<*>, distance(query, value.value, weights[i])))
                    }
                }
            }
        } else {
            tx.forEach(parallelism) {
                val value = it[vec_col]
                if (value != null) {
                    queries.forEachIndexed { i, query ->
                        knnSet[i].add(ComparablePair(it[id_col] as Value<*>, distance(query, value.value)))
                    }
                }
            }
        }
        return knnSet
    }

    /**
     * Performs a kNN lookup with [LongArray]s by scanning an [Entity] using the given [Entity.Tx]. Parallelism is determined based on available CPU's and the estimated size of the dataset.
     *
     * @param tx The [Entity.Tx] to use for the table scan.
     * @param id_col The column that acts as ID for the resulting table. Can be unique but uniqueness is not a requirement!
     * @param vec_col The column that acts as vector for the distance calculation. Must be of type [LongArrayType]!
     * @param k The k in kNN, i.e. the maximum number of entries to retain.
     * @param distance The [Distance] function that should be used for calculation.
     * @param queries List of query vectors. Can contain more than one vector, each of which must be of type [LongArray].
     * @param weights The optional weights. If a list is provided, then the number of weights must be the same as the number of queries.
     *
     * @return A list of [HeapSelect] objects, one for each query vector.
     */
    protected fun longKnn(tx: Entity.Tx, id_col: ColumnDef<*>, vec_col: ColumnDef<LongArray>, k: Int, distance: Distance, queries: List<LongArray>, weights: List<FloatArray>? = null) : List<HeapSelect<ComparablePair<Value<*>, Double>>>  {
        val knnSet = queries.map { HeapSelect<ComparablePair<Value<*>,Double>>(k) }
        val parallelism = determineParallelism(tx.count())
        if (weights != null) {
            tx.forEach(parallelism) {
                val value = it[vec_col]
                if (value != null) {
                    queries.forEachIndexed { i, query ->
                        knnSet[i].add(ComparablePair(it[id_col] as Value<*>, distance(query, value.value, weights[i])))
                    }
                }
            }
        } else {
            tx.forEach(parallelism) {
                val value = it[vec_col]
                if (value != null) {
                    queries.forEachIndexed { i, query ->
                        knnSet[i].add(ComparablePair(it[id_col] as Value<*>, distance(query, value.value)))
                    }
                }
            }
        }
        return knnSet
    }

    /**
     * Performs a kNN lookup with [IntArray]s by scanning an [Entity] using the given [Entity.Tx]. Parallelism is determined based on available CPU's and the estimated size of the dataset.
     *
     * @param tx The [Entity.Tx] to use for the table scan.
     * @param id_col The column that acts as ID for the resulting table. Can be unique but uniqueness is not a requirement!
     * @param vec_col The column that acts as vector for the distance calculation. Must be of type [IntArrayType]!
     * @param k The k in kNN, i.e. the maximum number of entries to retain.
     * @param distance The [Distance] function that should be used for calculation.
     * @param queries List of query vectors. Can contain more than one vector, each of which must be of type [IntArray].
     * @param weights The optional weights. If a list is provided, then the number of weights must be the same as the number of queries.
     *
     * @return A list of [HeapSelect] objects, one for each query vector.
     */
    protected fun intKnn(tx: Entity.Tx, id_col: ColumnDef<*>, vec_col: ColumnDef<IntArray>, k: Int, distance: Distance, queries: List<IntArray>, weights: List<FloatArray>? = null) : List<HeapSelect<ComparablePair<Value<*>, Double>>> {
        val knnSet = queries.map { HeapSelect<ComparablePair<Value<*>,Double>>(k) }
        val parallelism = determineParallelism(tx.count())
        if (weights != null) {
            tx.forEach(parallelism) {
                val value = it[vec_col]
                if (value != null) {
                    queries.forEachIndexed { i, query ->
                        knnSet[i].add(ComparablePair(it[id_col] as Value<*>, distance(query, value.value, weights[i])))
                    }
                }
            }
        } else {
            tx.forEach(parallelism) {
                val value = it[vec_col]
                if (value != null) {
                    queries.forEachIndexed { i, query ->
                        knnSet[i].add(ComparablePair(it[id_col] as Value<*>, distance(query, value.value)))
                    }
                }
            }
        }
        return knnSet
    }

    /**
     * Determines the amount of parallelism that should be applied when performing kNN.
     *
     * @param size The size of the collection.
     * @return Amount of parallelism (between 1 and Runtime.getRuntime().availableProcessors())
     */
    private fun determineParallelism(size: Long): Short = max(1, min(Runtime.getRuntime().availableProcessors(), Math.floorDiv(size, PARALLELISM_THRESHOLD).toInt())).toShort()
}