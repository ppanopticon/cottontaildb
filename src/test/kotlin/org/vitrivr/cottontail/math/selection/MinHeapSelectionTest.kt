package org.vitrivr.cottontail.math.selection

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Test cases for [MinHeapSelection]
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class MinHeapSelectionTest {
    val RANDOM = SplittableRandom()

    /**
     * Test [MinHeapSelection] with (primitive) [Double]s.
     */
    @ParameterizedTest
    @ValueSource(ints = [10, 100, 500, 1000])
    fun testPrimitive(size: Int) {
        val list = (0 until RANDOM.nextInt(size, 5 * size)).map { RANDOM.nextDouble() }
        val selection = MinHeapSelection<Double>(size)
        for (e in list) {
            selection.offer(e)
        }

        /* Sort list. */
        val sorted = list.sorted()

        /* Compare sorted list to MinHeapSelect. */
        Assertions.assertEquals(sorted[size - 1], selection.peek())
        for (i in 0 until size) {
            Assertions.assertEquals(sorted[i], selection[i])
        }
    }

    /**
     * Test [MinHeapSelection] with [ComparablePair]s.
     */
    @ParameterizedTest
    @ValueSource(ints = [10, 100, 500, 1000])
    fun testDistanceComparable(size: Int) {
        val list = (0 until RANDOM.nextInt(size, 5 * size)).map { ComparablePair(RANDOM.nextLong(), RANDOM.nextDouble()) }
        val selection = MinHeapSelection<ComparablePair<Long, Double>>(size)
        for (e in list) {
            selection.offer(e)
        }

        /* Sort list. */
        val sorted = list.sorted()

        /* Compare sorted list to MinHeapSelect. */
        Assertions.assertEquals(sorted[size - 1], selection.peek())
        for (i in 0 until size) {
            Assertions.assertEquals(sorted[i], selection[i])
        }
    }

    /**
     * Test [MinHeapSelection] speed with [ComparablePair]s.
     * Once by directly offering a [ComparablePair], once by checking if it would be added beforehand and then only
     * constructing the pair and offering it if it will be added
     */
    @ExperimentalTime
    @ParameterizedTest
    @ValueSource(ints = [10, 100, 500, 1000])
    fun testSelectionSpeed(k: Int) {
        val rng = SplittableRandom(1234L)
        val numEntries = 10000000
        val longList = (0 until numEntries).map { rng.nextLong() }
        val doubleList = (0 until numEntries).map { rng.nextDouble() }
        val selectionOffer = MinHeapSelection<ComparablePair<Long, Double>>(k)
        val selectionPeek = MinHeapSelection<ComparablePair<Long, Double>>(k)
        var timeOffer = Duration.ZERO
        var timePeek = Duration.ZERO
        for (i in longList.indices) {
            timeOffer += measureTime {
                selectionOffer.offer(
                    ComparablePair(
                        longList[i],
                        doubleList[i]
                    )
                )
            }
            timePeek += measureTime {
                if (selectionPeek.size < k || doubleList[i] < selectionPeek.peek()!!.second) selectionPeek.offer(
                    ComparablePair(longList[i], doubleList[i])
                )
            }
        }
        println("timeOffer: $timeOffer")
        println("timePeek: $timePeek")
    }
}