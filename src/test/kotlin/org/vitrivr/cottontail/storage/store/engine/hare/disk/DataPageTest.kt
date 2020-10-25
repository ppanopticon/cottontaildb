package org.vitrivr.cottontail.storage.store.engine.hare.disk

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.DataPage
import java.nio.BufferOverflowException
import java.nio.ByteBuffer
import kotlin.random.Random

class DataPageTest {

    private val random = Random(System.currentTimeMillis())

    @ParameterizedTest
    @ValueSource(ints = [4096, 8192, 16384, 32768])
    fun writeBytesBatch(pageSize: Int) {
        val buffer = ByteBuffer.allocate(pageSize)
        val page = DataPage(buffer)
        val bytes = ByteArray(random.nextInt(pageSize))
        random.nextBytes(bytes)

        /* Write bytes. */
        page.putBytes(0, bytes)
        val read = page.getBytes(0)

        for (i in bytes.indices) {
            assertEquals(bytes[i], read[i])
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [4096, 8192, 16384, 32768])
    fun writeBytesBatchTooLarge(pageSize: Int) {
        val buffer = ByteBuffer.allocate(pageSize)
        val page = DataPage(buffer)
        val bytes = ByteArray(pageSize + random.nextInt(pageSize))
        random.nextBytes(bytes)

        /* Write bytes. */
        assertThrows<BufferOverflowException> { page.putBytes(0, bytes) }
    }

    @ParameterizedTest
    @ValueSource(ints = [4096, 8192, 16384, 32768])
    fun writeInts(pageSize: Int) {
        val buffer = ByteBuffer.allocate(pageSize)
        val page = DataPage(buffer)
        val ints = IntArray(random.nextInt(pageSize/Int.SIZE_BYTES)) {
            random.nextInt()
        }

        /* Write ints. */
        for (i in ints.indices) {
            page.putInt(Int.SIZE_BYTES * i, ints[i])
        }

        /* Compare and read ints. */
        for (i in ints.indices) {
            assertEquals(ints[i], page.getInt(Int.SIZE_BYTES * i))
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [4096, 8192, 16384, 32768])
    fun writeFloats(pageSize: Int) {
        val buffer = ByteBuffer.allocate(pageSize)
        val page = DataPage(buffer)
        val floats = FloatArray(random.nextInt(pageSize/Int.SIZE_BYTES)) {
            random.nextFloat()
        }

        /* Write ints. */
        for (i in floats.indices) {
            page.putFloat(Int.SIZE_BYTES * i, floats[i])
        }

        /* Compare and read ints. */
        for (i in floats.indices) {
            assertEquals(floats[i], page.getFloat(Int.SIZE_BYTES * i))
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [4096, 8192, 16384, 32768])
    fun writeLongs(pageSize: Int) {
        val buffer = ByteBuffer.allocate(pageSize)
        val page = DataPage(buffer)
        val longs = LongArray(random.nextInt(pageSize/Long.SIZE_BYTES)) {
            random.nextLong()
        }

        /* Write ints. */
        for (i in longs.indices) {
            page.putLong(Long.SIZE_BYTES * i, longs[i])
        }

        /* Compare and read ints. */
        for (i in longs.indices) {
            assertEquals(longs[i], page.getLong(Long.SIZE_BYTES * i))
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [4096, 8192, 16384, 32768])
    fun writeDoubles(pageSize: Int) {
        val buffer = ByteBuffer.allocate(pageSize)
        val page = DataPage(buffer)
        val doubles = DoubleArray(random.nextInt(pageSize/Long.SIZE_BYTES)) {
            random.nextDouble()
        }

        /* Write ints. */
        for (i in doubles.indices) {
            page.putDouble(Long.SIZE_BYTES * i, doubles[i])
        }

        /* Compare and read ints. */
        for (i in doubles.indices) {
            assertEquals(doubles[i], page.getDouble(Long.SIZE_BYTES * i))
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [4096, 8192, 16384, 32768])
    fun writeOverflow(pageSize: Int) {
        val buffer = ByteBuffer.allocate(pageSize)
        val page = DataPage(buffer)
        assertThrows<IndexOutOfBoundsException> {
            page.putInt(pageSize + random.nextInt(pageSize), random.nextInt())
        }
        assertThrows<IndexOutOfBoundsException> {
            page.putLong(pageSize + random.nextInt(pageSize), random.nextLong())
        }
        assertThrows<IndexOutOfBoundsException> {
            page.putFloat(pageSize + random.nextInt(pageSize), random.nextFloat())
        }
        assertThrows<IndexOutOfBoundsException> {
            page.putDouble(pageSize + random.nextInt(pageSize), random.nextDouble())
        }
    }
}