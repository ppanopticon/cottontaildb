package org.vitrivr.cottontail.math.knn

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.math.isApproximatelyTheSame
import org.vitrivr.cottontail.math.knn.basics.DistanceKernel
import org.vitrivr.cottontail.math.knn.kernels.Distances
import org.vitrivr.cottontail.model.values.IntVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.VectorUtility
import kotlin.math.abs
import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Test cases that test for correctness of some basic distance calculations with [IntVectorDistanceTest].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class IntVectorDistanceTest : AbstractDistanceTest() {

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun testL1Distance(dimensions: Int) {
        val query = IntVectorValue.random(dimensions, RANDOM)
        val collection = VectorUtility.randomIntVectorSequence(dimensions, TestConstants.collectionSize, RANDOM)

        var sum1 = 0.0f
        var sum2 = 0.0f
        var sum3 = 0.0f

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        val kernel = Distances.L1.kernelForQuery(query) as DistanceKernel<VectorValue<*>>

        collection.forEach {
            time1 += measureTime {
                sum1 += kernel(it).value.toFloat()
            }
            time2 += measureTime {
                sum2 += (it - query).abs().sum().value
            }
            sum3 += l1(it.data, query.data)
        }

        println("Calculating L1 distance for collection (s=${TestConstants.collectionSize}, d=$dimensions) took ${time1 / TestConstants.collectionSize} (optimized) resp. ${time2 / TestConstants.collectionSize} per vector on average.")

        if (time1 > time2) {
            LOGGER.warn("Optimized version of L2^2 is slower than default version!")
        }
        isApproximatelyTheSame(sum3, sum1)
        isApproximatelyTheSame(sum3, sum2)
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun testL2SquaredDistance(dimensions: Int) {
        val query = IntVectorValue.random(dimensions, RANDOM)
        val collection = VectorUtility.randomIntVectorSequence(dimensions, TestConstants.collectionSize, RANDOM)

        var sum1 = 0.0f
        var sum2 = 0.0f
        var sum3 = 0.0f

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        val kernel = Distances.L2SQUARED.kernelForQuery(query) as DistanceKernel<VectorValue<*>>


        collection.forEach {
            time1 += measureTime {
                sum1 += kernel(it).value.toFloat()
            }
            time2 += measureTime {
                sum2 += (it - query).pow(2).sum().value
            }
            sum3 += l2squared(it.data, query.data)
        }

        println("Calculating L2^2 distance for collection (s=${TestConstants.collectionSize}, d=$dimensions) took ${time1 / TestConstants.collectionSize} (optimized) resp. ${time2 / TestConstants.collectionSize} per vector on average.")

        if (time1 > time2) {
            LOGGER.warn("Optimized version of L2^2 is slower than default version!")
        }
        isApproximatelyTheSame(sum3, sum1)
        isApproximatelyTheSame(sum3, sum2)
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun testL2Distance(dimensions: Int) {
        val query = IntVectorValue.random(dimensions, RANDOM)
        val collection = VectorUtility.randomIntVectorSequence(dimensions, TestConstants.collectionSize, RANDOM)

        var sum1 = 0.0f
        var sum2 = 0.0f
        var sum3 = 0.0f

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        val kernel = Distances.L2.kernelForQuery(query) as DistanceKernel<VectorValue<*>>

        collection.forEach {
            time1 += measureTime {
                sum1 += kernel(it).value.toFloat()
            }
            time2 += measureTime {
                sum2 += (query - it).pow(2).sum().sqrt().value
            }
            sum3 += l2(it.data, query.data)
        }

        println("Calculating L2 distance for collection (s=${TestConstants.collectionSize}, d=$dimensions) took ${time1 / TestConstants.collectionSize} (optimized) resp. ${time2 / TestConstants.collectionSize} per vector on average.")

        if (time1 > time2) {
            LOGGER.warn("Optimized version of L2^2 is slower than default version!")
        }
        isApproximatelyTheSame(sum3, sum1)
        isApproximatelyTheSame(sum3, sum2)
    }

    /**
     * Calculates the L<sub>1</sub> (sum of abs) distance between two points.
     *
     * @param p1 the first point
     * @param p2 the second point
     * @return the L<sub>1</sub> distance between the two points
     */
    fun l1(p1: IntArray, p2: IntArray): Float {
        require(p1.size == p2.size) { "Dimension mismatch!" }
        var sum = 0.0f
        for (i in p1.indices) {
            sum += abs(p1[i] - p2[i])
        }
        return sum
    }

    /**
     * Calculates the L<sub>2</sub> (Euclidean) distance between two points.
     *
     * @param p1 the first point
     * @param p2 the second point
     * @return the L<sub>2</sub> distance between the two points
     */
    private fun l2(p1: IntArray, p2: IntArray): Float {
        require(p1.size == p2.size) { "Dimension mismatch!" }
        var sum = 0.0f
        for (i in p1.indices) {
            val dp = p1[i] - p2[i]
            sum += dp.toFloat().pow(2)
        }
        return kotlin.math.sqrt(sum)
    }

    /**
     * Calculates the L<sub>2</sub> (Euclidean) distance between two points.
     *
     * @param p1 the first point
     * @param p2 the second point
     * @return the L<sub>2</sub> distance between the two points
     */
    private fun l2squared(p1: IntArray, p2: IntArray): Float {
        require(p1.size == p2.size) { "Dimension mismatch!" }
        var sum = 0.0f
        for (i in p1.indices) {
            val dp = p1[i] - p2[i]
            sum += dp.toFloat().pow(2)
        }
        return sum
    }
}