package org.vitrivr.cottontail.storage.store.engine.hare.views

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.vitrivr.cottontail.storage.engine.hare.SlotId
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.storage.engine.hare.views.SlottedPageView
import java.nio.ByteBuffer
import kotlin.random.Random

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */

class SlottedPageViewTest {

    private val random = Random(System.currentTimeMillis())

    @ParameterizedTest
    @ValueSource(ints = [4096, 8192, 16384, 32768])
    fun testWrapException(pageSize: Int) {
        val buffer = ByteBuffer.allocate(pageSize)
        val page = HarePage(buffer)
        Assertions.assertThrows(IllegalArgumentException::class.java) {
            SlottedPageView(page).validate()
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [4096, 8192, 16384, 32768])
    fun testFullAllocation(pageSize: Int) {
        val buffer = ByteBuffer.allocate(pageSize)
        val page = HarePage(buffer)
        SlottedPageView.initialize(page)
        val slotted = SlottedPageView(page).validate()

        /** Check vanilla page. */
        Assertions.assertEquals(0, slotted.slots)
        Assertions.assertEquals(pageSize, slotted.freePointer)
        Assertions.assertEquals(pageSize - SlottedPageView.SIZE_HEADER, slotted.freeSpace)

        /** Try allocation. */
        val allocationSize = slotted.freeSpace - SlottedPageView.SIZE_ENTRY
        val slotId = slotted.allocate(allocationSize)
        Assertions.assertNotNull(slotId)
        Assertions.assertEquals(1, slotted.slots)
        Assertions.assertEquals(0, slotted.freeSpace)
        Assertions.assertEquals(allocationSize, slotted.size(slotId!!.toShort()))
    }

    @ParameterizedTest
    @ValueSource(ints = [4096, 8192, 16384, 32768])
    fun testOverflowAllocation(pageSize: Int) {
        val buffer = ByteBuffer.allocate(pageSize)
        val page = HarePage(buffer)
        SlottedPageView.initialize(page)
        val slotted = SlottedPageView(page).validate()
        val allocationSize = slotted.freeSpace - SlottedPageView.SIZE_ENTRY + 1

        /** Check vanilla page. */
        Assertions.assertEquals(0, slotted.slots)
        Assertions.assertEquals(pageSize, slotted.freePointer)
        Assertions.assertEquals(pageSize - SlottedPageView.SIZE_HEADER, slotted.freeSpace)

        /** Try allocation. */
        val slotId = slotted.allocate(allocationSize)
        Assertions.assertNull(slotId)
        Assertions.assertEquals(0, slotted.slots)
        Assertions.assertEquals(page.size, slotted.freePointer)
        Assertions.assertEquals(page.size - SlottedPageView.SIZE_HEADER, slotted.freeSpace)
    }

    @ParameterizedTest
    @ValueSource(ints = [4096, 8192, 16384, 32768])
    fun testRandomWriteRead(pageSize: Int) {
        val buffer = ByteBuffer.allocate(pageSize)
        val page = HarePage(buffer)
        SlottedPageView.initialize(page)
        val slotted = SlottedPageView(page).validate()

        /** Check vanilla page. */
        Assertions.assertEquals(0, slotted.slots)
        Assertions.assertEquals(pageSize, slotted.freePointer)
        Assertions.assertEquals(pageSize - SlottedPageView.SIZE_HEADER, slotted.freeSpace)

        /** Populate slotted page with random data. */
        val list = mutableListOf<Pair<SlotId, ByteArray>>()
        var i = 0
        while (true) {
            val minSize = 4
            val maxSize = slotted.freeSpace - SlottedPageView.SIZE_ENTRY
            if (maxSize < minSize) {
                break
            }
            val allocate = this.random.nextInt(minSize, maxSize)
            val slotId = slotted.allocate(allocate) ?: break
            val offset = slotted.offset(slotId)
            val data: ByteArray = this.random.nextBytes(allocate)
            slotted.page.putBytes(offset, data)
            list.add(Pair(slotId, data))

            Assertions.assertEquals(++i, slotted.slots)
            Assertions.assertEquals(allocate, slotted.size(slotId))
        }

        /** Check data that was written. */
        for (entry in list) {
            val offset = slotted.offset(entry.first)
            val read = slotted.page.getBytes(offset, offset + entry.second.size)
            Assertions.assertArrayEquals(entry.second, read)
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [4096, 8192, 16384, 32768])
    fun testFree(pageSize: Int) {
        TODO()
    }
}