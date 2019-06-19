package ch.unibas.dmi.dbis.cottontail.calcite.knn

import ch.unibas.dmi.dbis.cottontail.math.knn.ComparablePair
import ch.unibas.dmi.dbis.cottontail.math.knn.HeapSelect
import ch.unibas.dmi.dbis.cottontail.math.knn.metrics.Distance
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.values.*
import org.apache.calcite.DataContext

import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.Enumerator
import org.apache.calcite.linq4j.Linq4j
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.schema.QueryableTable
import org.apache.calcite.schema.ScannableTable
import org.apache.calcite.schema.Schema
import org.apache.calcite.schema.Statistic
import org.apache.calcite.schema.impl.TableFunctionImpl


/**
 * This class represents a function for kNN lookup. It can be used by Apache Calcite as a [TableFunctionImpl].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class CottontailKnnFunction {


    companion object {
        const val KEY_FIELD_INDEX = 0
        const val VECTOR_FIELD_INDEX = 1
    }

    /**
     * Executes the kNN lookup on the given input.
     *
     * @param source The data source. Usually an enumerable stemming from a cursor (i.e. another query).
     * @param k The k in kNN, i.e. the top k results will be kept. Defaults to 250
     * @param distance The name of the distance function to use. See [Distance]. Defaults to L2
     * @param query A list of query vectors. The kNN results is calculated for each query vector.
     * @param weights List of weights that should be used for kNN.
     */
    fun evaluate(source: Enumerable<Array<Value<*>?>>, query: List<Array<Number>>, k: Int = 250, distance: String = "L2", weights: List<Array<Number>>? = null) : CottontailKnnTable {
        val results = when(source.first()[VECTOR_FIELD_INDEX]) {
            is DoubleArrayValue -> doubleKnn(source = source.enumerator(), k = k, distance = Distance.valueOf(distance), queries = query.map { array -> DoubleArray(array.size) { array[it].toDouble() } }, weights = weights?.map { array -> DoubleArray(array.size) { array[it].toDouble() } })
            is FloatArrayValue -> floatKnn(source = source.enumerator(), k = k,  distance = Distance.valueOf(distance), queries = query.map {array -> FloatArray(array.size) { array[it].toFloat() } }, weights = weights?.map { array -> FloatArray(array.size) { array[it].toFloat() } })
            is LongArrayValue -> longKnn(source = source.enumerator(), k = k,  distance = Distance.valueOf(distance), queries = query.map {array -> LongArray(array.size) { array[it].toLong() } }, weights = weights?.map { array -> FloatArray(array.size) { array[it].toFloat() } })
            is IntArrayValue -> intKnn(source = source.enumerator(),k = k, distance = Distance.valueOf(distance), queries = query.map {array -> IntArray(array.size) { array[it].toInt() } }, weights = weights?.map { array -> FloatArray(array.size) { array[it].toFloat() } })
            else -> throw QueryException.QueryBindException("Column of type ${source.first()[VECTOR_FIELD_INDEX]} is not supported for kNN queries.")
        }

        /* Return a Enumerator. */
        return CottontailKnnTable(results)
    }

    /**
     *
     */
    private fun doubleKnn(source: Enumerator<Array<Value<*>?>>, k: Int, distance: Distance, queries: List<DoubleArray>, weights: List<DoubleArray>?) : List<HeapSelect<ComparablePair<Value<*>,Double>>>  {
        val knnSet = queries.map { HeapSelect<ComparablePair<Value<*>,Double>>(k) }
        if (weights != null) {
            var current: Array<Value<*>?>?
            loop@ while (true) {
                synchronized(source) {
                    if (source.moveNext()) {
                        current = source.current()
                    } else {
                        current = null
                    }
                }
                if (current == null) {
                    break@loop
                }
                queries.forEachIndexed { i, q ->
                    val  dist = distance.invoke(current!![VECTOR_FIELD_INDEX]!!.value as DoubleArray, q, weights[i])
                    knnSet[i].add(ComparablePair(current!![KEY_FIELD_INDEX]!!, dist))
                }
            }
        } else {
            var current: Array<Value<*>?>?
            loop@ while (true) {
                synchronized(source) {
                    if (source.moveNext()) {
                        current = source.current()
                    } else {
                        current = null
                    }
                }
                if (current == null) {
                    break@loop
                }
                queries.forEachIndexed { i, q ->
                    val  dist = distance.invoke(current!![VECTOR_FIELD_INDEX]!!.value as DoubleArray, q)
                    knnSet[i].add(ComparablePair(current!![KEY_FIELD_INDEX]!!, dist))
                }
            }
        }
        return knnSet
    }

    /**
     *
     */
    private fun floatKnn(source: Enumerator<Array<Value<*>?>>, k: Int, distance: Distance, queries: List<FloatArray>,  weights: List<FloatArray>? = null) : List<HeapSelect<ComparablePair<Value<*>,Double>>>  {
        val knnSet = queries.map { HeapSelect<ComparablePair<Value<*>,Double>>(k) }
        if (weights != null) {
            var current: Array<Value<*>?>?
            loop@ while (true) {
                synchronized(source) {
                    if (source.moveNext()) {
                        current = source.current()
                    } else {
                        current = null
                    }
                }
                if (current == null) {
                    break@loop
                }
                queries.forEachIndexed { i, q ->
                    val  dist = distance.invoke(current!![VECTOR_FIELD_INDEX]!!.value as FloatArray, q, weights[i])
                    knnSet[i].add(ComparablePair(current!![KEY_FIELD_INDEX]!!, dist))
                }
            }
        } else {
            var current: Array<Value<*>?>?
            loop@ while (true) {
                synchronized(source) {
                    if (source.moveNext()) {
                        current = source.current()
                    } else {
                        current = null
                    }
                }
                if (current == null) {
                    break@loop
                }
                queries.forEachIndexed { i, q ->
                    val  dist = distance.invoke(current!![VECTOR_FIELD_INDEX]!!.value as FloatArray, q)
                    knnSet[i].add(ComparablePair(current!![KEY_FIELD_INDEX]!!, dist))
                }
            }
        }
        return knnSet
    }

    /**
     *
     */
    private fun longKnn(source: Enumerator<Array<Value<*>?>>, k: Int, distance: Distance, queries: List<LongArray>, weights: List<FloatArray>? = null) : List<HeapSelect<ComparablePair<Value<*>,Double>>>  {
        val knnSet = queries.map { HeapSelect<ComparablePair<Value<*>,Double>>(k) }
        if (weights != null) {
            var current: Array<Value<*>?>?
            loop@ while (true) {
                synchronized(source) {
                    if (source.moveNext()) {
                        current = source.current()
                    } else {
                        current = null
                    }
                }
                if (current == null) {
                    break@loop
                }
                queries.forEachIndexed { i, q ->
                    val  dist = distance.invoke(current!![VECTOR_FIELD_INDEX]!!.value as LongArray, q, weights[i])
                    knnSet[i].add(ComparablePair(current!![KEY_FIELD_INDEX]!!, dist))
                }
            }
        } else {
            var current: Array<Value<*>?>?
            loop@ while (true) {
                synchronized(source) {
                    if (source.moveNext()) {
                        current = source.current()
                    } else {
                        current = null
                    }
                }
                if (current == null) {
                    break@loop
                }
                queries.forEachIndexed { i, q ->
                    val  dist = distance.invoke(current!![VECTOR_FIELD_INDEX]!!.value as LongArray, q)
                    knnSet[i].add(ComparablePair(current!![KEY_FIELD_INDEX]!!, dist))
                }
            }
        }
        return knnSet
    }

    /**
     *
     */
    private fun intKnn(source: Enumerator<Array<Value<*>?>>, k: Int, distance: Distance, queries: List<IntArray>, weights: List<FloatArray>? = null) : List<HeapSelect<ComparablePair<Value<*>,Double>>> {
        val knnSet = queries.map { HeapSelect<ComparablePair<Value<*>,Double>>(k) }
        if (weights != null) {
            var current: Array<Value<*>?>?
            loop@ while (true) {
                synchronized(source) {
                    if (source.moveNext()) {
                        current = source.current()
                    } else {
                        current = null
                    }
                }
                if (current == null) {
                    break@loop
                }
                queries.forEachIndexed { i, q ->
                    val  dist = distance.invoke(current!![VECTOR_FIELD_INDEX]!!.value as IntArray, q, weights[i])
                    knnSet[i].add(ComparablePair(current!![KEY_FIELD_INDEX]!!, dist))
                }
            }
        } else {
            var current: Array<Value<*>?>?
            loop@ while (true) {
                synchronized(source) {
                    if (source.moveNext()) {
                        current = source.current()
                    } else {
                        current = null
                    }
                }
                if (current == null) {
                    break@loop
                }
                queries.forEachIndexed { i, q ->
                    val  dist = distance.invoke(current!![VECTOR_FIELD_INDEX]!!.value as IntArray, q)
                    knnSet[i].add(ComparablePair(current!![KEY_FIELD_INDEX]!!, dist))
                }
            }
        }
        return knnSet
    }
}