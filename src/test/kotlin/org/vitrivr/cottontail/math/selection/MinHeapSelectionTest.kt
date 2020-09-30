package org.vitrivr.cottontail.math.selection

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import java.util.*

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
}