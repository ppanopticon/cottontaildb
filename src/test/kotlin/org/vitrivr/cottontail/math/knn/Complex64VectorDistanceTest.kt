package org.vitrivr.cottontail.math.knn

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.math.absFromFromComplexFieldVector
import org.vitrivr.cottontail.math.arrayFieldVectorFromVectorValue
import org.vitrivr.cottontail.math.conjFromFromComplexFieldVector
import org.vitrivr.cottontail.math.isApproximatelyTheSame
import org.vitrivr.cottontail.math.knn.metrics.EuclidianDistance
import org.vitrivr.cottontail.math.knn.metrics.ManhattanDistance
import org.vitrivr.cottontail.math.knn.metrics.RealInnerProductDistance
import org.vitrivr.cottontail.model.values.Complex64VectorValue
import org.vitrivr.cottontail.utilities.VectorUtility
import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Test cases that test for correctness of some basic distance calculations with [Complex64VectorValue].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class Complex64VectorDistanceTest : AbstractDistanceTest() {

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun testL1Distance(dimension: Int) {
        val query = Complex64VectorValue.random(dimension, RANDOM)
        val queryp = arrayFieldVectorFromVectorValue(query)
        val collection = VectorUtility.randomComplex64VectorSequence(dimension, TestConstants.collectionSize, RANDOM)

        var sum1 = 0.0
        var sum2 = 0.0
        var sum3 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        collection.forEach {
            time1 += measureTime {
                sum1 += ManhattanDistance(it, query).value
            }
            time2 += measureTime {
                sum2 += (query - it).abs().sum().value
            }
            val dataitem = arrayFieldVectorFromVectorValue(it)
            sum3 += absFromFromComplexFieldVector(queryp.subtract(dataitem)).l1Norm
        }

        println("Calculating L1 distance for collection (s=$TestConstants.collectionSize, d=$dimension) took ${time1 / TestConstants.collectionSize} (optimized) resp. ${time2 / TestConstants.collectionSize}  per vector on average.")

        if (time1 > time2) {
            LOGGER.warn("Optimized version of L1 is slower than default version!")
        }
        isApproximatelyTheSame(sum3, sum1)
        isApproximatelyTheSame(sum3, sum2)
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun testL2SquaredDistance(dimension: Int) {
        val query = Complex64VectorValue.random(dimension, RANDOM)
        val collection = VectorUtility.randomComplex64VectorSequence(dimension, TestConstants.collectionSize, RANDOM)
        val queryp = arrayFieldVectorFromVectorValue(query)

        var sum1 = 0.0
        var sum2 = 0.0
        var sum3 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        collection.forEach {
            time1 += measureTime {
                sum1 += (it.l2sq(query)).value
            }
            time2 += measureTime {
                sum2 += (query - it).abs().pow(2).sum().value
            }
            val dataitem = arrayFieldVectorFromVectorValue(it)
            sum3 += absFromFromComplexFieldVector(queryp.subtract(dataitem)).norm.pow(2)
        }

        println("Calculating L2^2 distance for collection (s=$TestConstants.collectionSize, d=$dimension) took ${time1 / TestConstants.collectionSize} (optimized) resp. ${time2 / TestConstants.collectionSize}  per vector on average.")

        if (time1 > time2) {
            LOGGER.warn("Optimized version of L1 is slower than default version!")
        }
        isApproximatelyTheSame(sum3, sum1)
        isApproximatelyTheSame(sum3, sum2)
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun testL2Distance(dimension: Int) {
        val query = Complex64VectorValue.random(dimension, RANDOM)
        val queryp = arrayFieldVectorFromVectorValue(query)
        val collection = VectorUtility.randomComplex64VectorSequence(dimension, TestConstants.collectionSize, RANDOM)

        var sum1 = 0.0
        var sum2 = 0.0
        var sum3 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        collection.forEach {
            time1 += measureTime {
                sum1 += EuclidianDistance(it, query).value
            }
            time2 += measureTime {
                sum2 += (query - it).abs().pow(2).sum().sqrt().value
            }
            val dataitem = arrayFieldVectorFromVectorValue(it)
            sum3 += absFromFromComplexFieldVector(queryp.subtract(dataitem)).norm
        }

        println("Calculating L2 distance for collection (s=${TestConstants.collectionSize}, d=$dimension) took ${time1 / TestConstants.collectionSize} (optimized) resp. ${time2 / TestConstants.collectionSize} per vector on average.")

        if (time1 > time2) {
            LOGGER.warn("Optimized version of L1 is slower than default version!")
        }
        isApproximatelyTheSame(sum3, sum1)
        isApproximatelyTheSame(sum3, sum2)
    }

    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun testIPRealDistance(dimension: Int) {
        val query = Complex64VectorValue.random(dimension, RANDOM)
        val queryp = arrayFieldVectorFromVectorValue(query)
        val collection = VectorUtility.randomComplex64VectorSequence(dimension, TestConstants.collectionSize, RANDOM)

        var sum1 = 0.0
        var sum2 = 0.0
        var sum3 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        collection.forEach {
            val conjDataitem = conjFromFromComplexFieldVector(arrayFieldVectorFromVectorValue(it))
            time1 += measureTime {
                sum1 += 1.0 - query.dotRealPart(it).value
            }
            time2 += measureTime {
                sum2 += RealInnerProductDistance(query, it).value
            }
            sum3 += 1.0 - queryp.dotProduct(conjDataitem).real
        }

        println("Calculating abs of DOT for collection (s=${TestConstants.collectionSize}, d=$dimension) took ${time1 / TestConstants.collectionSize} (optimized) resp. ${time2 / TestConstants.collectionSize} per vector on average.")

        if (time1 > time2) {
            LOGGER.warn("Optimized version of L1 is slower than default version!")
        }
        isApproximatelyTheSame(sum3, sum1)
        isApproximatelyTheSame(sum3, sum2)
    }
}