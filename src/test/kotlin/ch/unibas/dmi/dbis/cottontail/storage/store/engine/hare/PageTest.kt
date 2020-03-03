package ch.unibas.dmi.dbis.cottontail.storage.store.engine.hare

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page.Constants.PAGE_DATA_SIZE_BYTES
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*

import org.junit.jupiter.api.assertThrows
import java.nio.BufferOverflowException

import java.nio.ByteBuffer
import kotlin.random.Random

class PageTest {

    private val random = Random(System.currentTimeMillis())

    @Test
    fun testPageConstructorCorrect() {
        val buffer = ByteBuffer.allocate(PAGE_DATA_SIZE_BYTES + Page.Constants.PAGE_HEADER_SIZE_BYTES)
        val page =  Page(buffer)
        assertFalse(page.dirty)
        assertEquals(0L, page.id)
        assertEquals(Page.Priority.DEFAULT, page.priority)
    }

    @RepeatedTest(10)
    fun testPageConstructorIncorrect() {
        val buffer = ByteBuffer.allocate(random.nextInt(8192))
        if (buffer.capacity() != (PAGE_DATA_SIZE_BYTES + Page.Constants.PAGE_HEADER_SIZE_BYTES)) {
            assertThrows<IllegalArgumentException> { Page(buffer) }
        }
    }

    @RepeatedTest(10)
    fun writeBytesBatch() {
        val buffer = ByteBuffer.allocate(Page.Constants.PAGE_DATA_SIZE_BYTES + Page.Constants.PAGE_HEADER_SIZE_BYTES)
        val page = Page(buffer)
        val bytes = ByteArray(random.nextInt(PAGE_DATA_SIZE_BYTES))
        random.nextBytes(bytes)

        /* Write bytes. */
        page.putBytes(0, bytes)
        assertTrue(page.dirty) /* Check dirty flag. */
        val read = page.getBytes(0)

        for (i in bytes.indices) {
            assertEquals(bytes[i], read[i])
        }
    }

    @RepeatedTest(10)
    fun writeBytesBatchTooLarge() {
        val buffer = ByteBuffer.allocate(Page.Constants.PAGE_DATA_SIZE_BYTES + Page.Constants.PAGE_HEADER_SIZE_BYTES)
        val page = Page(buffer)
        val bytes = ByteArray(PAGE_DATA_SIZE_BYTES + random.nextInt(PAGE_DATA_SIZE_BYTES))
        random.nextBytes(bytes)

        /* Write bytes. */
        assertThrows<BufferOverflowException> { page.putBytes(0, bytes) }
        assertFalse(page.dirty) /* Check dirty flag. */
    }

    @RepeatedTest(10)
    fun writeInts() {
        val buffer = ByteBuffer.allocate(Page.Constants.PAGE_DATA_SIZE_BYTES + Page.Constants.PAGE_HEADER_SIZE_BYTES)
        val page = Page(buffer)
        val ints = IntArray(random.nextInt(PAGE_DATA_SIZE_BYTES/Int.SIZE_BYTES)) {
            random.nextInt()
        }

        /* Write ints. */
        for (i in ints.indices) {
            page.putInt(Int.SIZE_BYTES * i, ints[i])
        }
        assertTrue(page.dirty) /* Check dirty flag. */


        /* Compare and read ints. */
        for (i in ints.indices) {
            assertEquals(ints[i], page.getInt(Int.SIZE_BYTES * i))
        }
    }

    @RepeatedTest(10)
    fun writeFloats() {
        val buffer = ByteBuffer.allocate(Page.Constants.PAGE_DATA_SIZE_BYTES + Page.Constants.PAGE_HEADER_SIZE_BYTES)
        val page = Page(buffer)
        val floats = FloatArray(random.nextInt(PAGE_DATA_SIZE_BYTES/Int.SIZE_BYTES)) {
            random.nextFloat()
        }

        /* Write ints. */
        for (i in floats.indices) {
            page.putFloat(Int.SIZE_BYTES * i, floats[i])
        }
        assertTrue(page.dirty) /* Check dirty flag. */

        /* Compare and read ints. */
        for (i in floats.indices) {
            assertEquals(floats[i], page.getFloat(Int.SIZE_BYTES * i))
        }
    }

    @RepeatedTest(10)
    fun writeLongs() {
        val buffer = ByteBuffer.allocate(Page.Constants.PAGE_DATA_SIZE_BYTES + Page.Constants.PAGE_HEADER_SIZE_BYTES)
        val page = Page(buffer)
        val longs = LongArray(random.nextInt(PAGE_DATA_SIZE_BYTES/Long.SIZE_BYTES)) {
            random.nextLong()
        }

        /* Write ints. */
        for (i in longs.indices) {
            page.putLong(Long.SIZE_BYTES * i, longs[i])
        }
        assertTrue(page.dirty) /* Check dirty flag. */

        /* Compare and read ints. */
        for (i in longs.indices) {
            assertEquals(longs[i], page.getLong(Long.SIZE_BYTES * i))
        }
    }

    @RepeatedTest(10)
    fun writeDoubles() {
        val buffer = ByteBuffer.allocate(Page.Constants.PAGE_DATA_SIZE_BYTES + Page.Constants.PAGE_HEADER_SIZE_BYTES)
        val page = Page(buffer)
        val doubles = DoubleArray(random.nextInt(PAGE_DATA_SIZE_BYTES/Long.SIZE_BYTES)) {
            random.nextDouble()
        }

        /* Write ints. */
        for (i in doubles.indices) {
            page.putDouble(Long.SIZE_BYTES * i, doubles[i])
        }
        assertTrue(page.dirty) /* Check dirty flag. */

        /* Compare and read ints. */
        for (i in doubles.indices) {
            assertEquals(doubles[i], page.getDouble(Long.SIZE_BYTES * i))
        }
    }

    @RepeatedTest(10)
    fun writeOverflow() {
        val buffer = ByteBuffer.allocate(Page.Constants.PAGE_DATA_SIZE_BYTES + Page.Constants.PAGE_HEADER_SIZE_BYTES)
        val page = Page(buffer)
        assertThrows<IndexOutOfBoundsException> {
            page.putInt(PAGE_DATA_SIZE_BYTES + random.nextInt(PAGE_DATA_SIZE_BYTES), random.nextInt())
        }
        assertThrows<IndexOutOfBoundsException> {
            page.putLong(PAGE_DATA_SIZE_BYTES + random.nextInt(PAGE_DATA_SIZE_BYTES), random.nextLong())
        }
        assertThrows<IndexOutOfBoundsException> {
            page.putFloat(PAGE_DATA_SIZE_BYTES + random.nextInt(PAGE_DATA_SIZE_BYTES), random.nextFloat())
        }
        assertThrows<IndexOutOfBoundsException> {
            page.putDouble(PAGE_DATA_SIZE_BYTES + random.nextInt(PAGE_DATA_SIZE_BYTES), random.nextDouble())
        }
        assertFalse(page.dirty) /* Check dirty flag. */
    }
}