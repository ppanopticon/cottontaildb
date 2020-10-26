package org.vitrivr.cottontail.storage.store.engine.hare.disk

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.DataPage
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.LongStack

import java.nio.ByteBuffer
import java.util.*

/**
 * Test case for [LongStack] data structure.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class LongStackTest {
    /**
     * Offers random [Long]s to the [LongStack] and then pops and compares the values.
     */
    @ParameterizedTest()
    @ValueSource(ints = [512, 2048, 4096])
    fun testPop(size: Int) {
        val stack = LongStack(ByteBuffer.allocate(size))
        val random = SplittableRandom()
        val list = mutableListOf<Long>()

        /** Append random values. */
        var i1 = 0
        while (stack.entries < stack.capacity) {
            val next = random.nextLong()
            list.add(next)
            Assertions.assertTrue(stack.offer(next))
            Assertions.assertEquals(stack.entries, ++i1)
        }

        /** Test values. */
        var i2 = list.size - 1
        while (stack.entries > 0) {
            Assertions.assertEquals(list[i2], stack.pop())
            i2--
        }
    }

    /**
     * Offers random [Long]s to the [LongStack] and then checks, if [LongStack.contains] returns true for them.
     */
    @ParameterizedTest()
    @ValueSource(ints = [512, 2048, 4096])
    fun testContains(size: Int) {
        val stack = LongStack(ByteBuffer.allocate(size))
        val random = SplittableRandom()
        val list = mutableListOf<Long>()

        /** Append random values. */
        var i1 = 0
        while (stack.entries < stack.capacity) {
            val next = random.nextLong()
            list.add(next)
            Assertions.assertTrue(stack.offer(next))
            Assertions.assertEquals(stack.entries, ++i1)
        }

        /** Test positive (i.e. matching) values. */
        for (v in list) {
            Assertions.assertTrue(stack.contains(v))
        }
        /** Test negative (i.e. non-matching) values. */
        repeat(10) {
            Assertions.assertFalse(stack.contains(random.nextLong()))
        }
    }

    /**
     * Appends [DataPage]s of random bytes and checks, if those [DataPage]s' content remains the same after reading.
     */
    @ParameterizedTest()
    @ValueSource(ints = [512, 2048, 4096])
    fun testOverflow(size: Int) {
        val stack = LongStack(ByteBuffer.allocate(size))
        val random = SplittableRandom()
        val list = mutableListOf<Long>()

        /** Append random values. */
        var i1 = 0
        while (stack.entries < stack.capacity) {
            val next = random.nextLong()
            list.add(next)

            Assertions.assertTrue(stack.offer(next))
            Assertions.assertEquals(stack.entries, ++i1)
        }

        /** Append overflow value. */
        Assertions.assertFalse(stack.offer(random.nextLong()))
    }
}