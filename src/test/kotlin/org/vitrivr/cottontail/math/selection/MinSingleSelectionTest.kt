package org.vitrivr.cottontail.math.selection

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinSingleSelection
import java.util.*

/**
 * Test cases for [MinSingleSelection]
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class MinSingleSelectionTest {
    val RANDOM = SplittableRandom()

    val MAX_SIZE = 5000

    /**
     * Test [MinSingleSelection] with (primitive) [Double]s.
     */
    @RepeatedTest(100)
    fun testPrimitive() {
        val list = (0 until RANDOM.nextInt(MAX_SIZE)).map { RANDOM.nextDouble() }
        val selection = MinSingleSelection<Double>()
        for (e in list) {
            selection.offer(e)
        }

        /* Sort list. */
        val sorted = list.sorted()

        /* Compare sorted list to MinSingleSelect. */
        Assertions.assertEquals(sorted[0], selection[0])
        Assertions.assertEquals(sorted[0], selection.peek())
    }

    /**
     * Test [MinSingleSelection] with [ComparablePair]s.
     */
    @RepeatedTest(100)
    fun testDistanceComparable() {
        val list = (0 until RANDOM.nextInt(MAX_SIZE)).map { ComparablePair(RANDOM.nextLong(), RANDOM.nextDouble()) }
        val selection = MinSingleSelection<ComparablePair<Long, Double>>()
        for (e in list) {
            selection.offer(e)
        }

        /* Sort list. */
        val sorted = list.sorted()

        /* Compare sorted list to MinSingleSelect. */
        Assertions.assertEquals(sorted[0], selection[0])
        Assertions.assertEquals(sorted[0], selection.peek())
    }
}