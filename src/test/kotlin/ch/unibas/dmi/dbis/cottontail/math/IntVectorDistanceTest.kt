package ch.unibas.dmi.dbis.cottontail.math
/*
import ch.unibas.dmi.dbis.cottontail.math.knn.metrics.EuclidianDistance
import ch.unibas.dmi.dbis.cottontail.math.knn.metrics.ManhattanDistance
import ch.unibas.dmi.dbis.cottontail.math.knn.metrics.SquaredEuclidianDistance
import ch.unibas.dmi.dbis.cottontail.utilities.VectorUtility
import org.apache.commons.math3.util.MathArrays
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
import java.util.*
import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

class IntVectorDistanceTest {

    companion object {
        const val COLLECTION_SIZE = 1_000_000
        const val DELTA = 1e-12
        val RANDOM = SplittableRandom()
    }

    @ExperimentalTime
    @RepeatedTest(3)
    fun testL1Distance() {
        val dimensions = RANDOM.nextInt(2048)
        val query = VectorUtility.randomIntVector(dimensions)
        val collection = VectorUtility.randomIntVectorSequence(dimensions, COLLECTION_SIZE)

        var sum1 = 0.0
        var sum2 = 0.0
        var sum3 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        collection.forEach {
            time1 += measureTime {
                sum1 += ManhattanDistance(it, query)
            }
            time2 += measureTime {
                sum2 += (it - query).abs().sum().value.toDouble()
            }
            sum3 += MathArrays.distance1(it.data, query.data)
        }

        println("Calculating L1 distance for collection (s=$COLLECTION_SIZE, d=$dimensions) took ${time1/ COLLECTION_SIZE} (optimized) resp. ${time2/ COLLECTION_SIZE} per vector on average.")

        Assertions.assertTrue(time1 < time2, "Optimized version of L1 is slower than default version!")
        Assertions.assertTrue(sum1 / sum3 < 1.0 + DELTA, "Deviation for optimized version detected. Expected: $sum3, Received: $sum1")
        Assertions.assertTrue(sum1 / sum3 > 1.0 - DELTA, "Deviation for optimized version detected. Expected: $sum3, Received: $sum1")
        Assertions.assertTrue(sum2 / sum3 < 1.0 + DELTA, "Deviation for manual version detected. Expected: $sum3, Received: $sum2")
        Assertions.assertTrue(sum2 / sum3 > 1.0 - DELTA, "Deviation for manual version detected. Expected: $sum3, Received: $sum2")
    }

    @ExperimentalTime
    @RepeatedTest(3)
    fun testL2SquaredDistance() {
        val dimensions = RANDOM.nextInt(2048)
        val query = VectorUtility.randomIntVector(dimensions)
        val collection = VectorUtility.randomIntVectorSequence(dimensions, COLLECTION_SIZE)

        var sum1 = 0.0
        var sum2 = 0.0
        var sum3 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        collection.forEach {
            time1 += measureTime {
                sum1 += SquaredEuclidianDistance(it, query)
            }
            time2 += measureTime {
                sum2 += (it-query).pow(2).sum().value.toDouble()
            }
            sum3 += MathArrays.distance(it.data, query.data).pow(2)
        }

        println("Calculating L2^2 distance for collection (s=${DoubleVectorDistanceTest.COLLECTION_SIZE}, d=$dimensions) took ${time1/ COLLECTION_SIZE} (optimized) resp. ${time2/ COLLECTION_SIZE} per vector on average.")

        Assertions.assertTrue(time1 < time2, "Optimized version of L2^2 is slower than default version!")
        Assertions.assertTrue(sum1 / sum3 < 1.0 + DELTA, "Deviation for optimized version detected. Expected: $sum3, Received: $sum1")
        Assertions.assertTrue(sum1 / sum3 > 1.0 - DELTA, "Deviation for optimized version detected. Expected: $sum3, Received: $sum1")
        Assertions.assertTrue(sum2 / sum3 < 1.0 + DELTA, "Deviation for manual version detected. Expected: $sum3, Received: $sum2")
        Assertions.assertTrue(sum2 / sum3 > 1.0 - DELTA, "Deviation for manual version detected. Expected: $sum3, Received: $sum2")
    }

    @ExperimentalTime
    @RepeatedTest(3)
    fun testL2Distance() {
        val dimensions = RANDOM.nextInt(2048)
        val query = VectorUtility.randomIntVector(dimensions)
        val collection = VectorUtility.randomIntVectorSequence(dimensions, COLLECTION_SIZE)

        var sum1 = 0.0
        var sum2 = 0.0
        var sum3 = 0.0

        var time1 = Duration.ZERO
        var time2 = Duration.ZERO

        collection.forEach {
            time1 += measureTime {
                sum1 += EuclidianDistance(it, query)
            }
            time2 += measureTime {
                sum2 += (query-it).pow(2).sum().sqrt().value
            }
            sum3 += MathArrays.distance(it.data, query.data)
        }

        println("Calculating L2 distance for collection (s=$COLLECTION_SIZE, d=$dimensions) took ${time1/ COLLECTION_SIZE} (optimized) resp. ${time2/ COLLECTION_SIZE} per vector on average.")

        Assertions.assertTrue(time1 < time2, "Optimized version of L2 is slower than default version!")
        Assertions.assertTrue(sum1 / sum3 < 1.0 + DELTA, "Deviation for optimized version detected. Expected: $sum3, Received: $sum1")
        Assertions.assertTrue(sum1 / sum3 > 1.0 - DELTA, "Deviation for optimized version detected. Expected: $sum3, Received: $sum1")
        Assertions.assertTrue(sum2 / sum3 < 1.0 + DELTA, "Deviation for manual version detected. Expected: $sum3, Received: $sum2")
        Assertions.assertTrue(sum2 / sum3 > 1.0 - DELTA, "Deviation for manual version detected. Expected: $sum3, Received: $sum2")
    }
}*/