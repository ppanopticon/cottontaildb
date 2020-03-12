package ch.unibas.dmi.dbis.cottontail.storage.store.engine.hare

import ch.unibas.dmi.dbis.cottontail.storage.basics.Units
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.*

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach

import java.nio.file.Paths
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.RepeatedTest
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

class DirectDiskManagerTest {
    val path = Paths.get("./test-diskmgr-db.hare")

    var manager: DirectDiskManager? = null

    val random = SplittableRandom(System.currentTimeMillis())

    @BeforeEach
    fun beforeEach() {
        DiskManager.create(this.path)
        this.manager = DirectDiskManager(this.path)
    }

    @AfterEach
    fun afterEach() {
        this.manager!!.close()
        Files.delete(this.path)
    }

    @Test
    fun testCreationAndLoading() {
        assertEquals(this.path, this.manager!!.path)
        assertEquals(0, this.manager!!.pages)
        assertEquals(Page.Constants.PAGE_DATA_SIZE_BYTES, this.manager!!.size.value.toInt())
    }

    /**
     * Appends [Page]s of random bytes and checks, if those [Page]s' content remains the same after reading.
     */
    @ExperimentalTime
    @RepeatedTest(5)
    fun testAppendPage() {
        val data = this.initWithData(random.nextInt(65536))

        /* Check if data remains the same. */
        this.compareData(data)
    }

    /**
     * Appends [Page]s of random bytes and checks, if those [Page]s' content remains the same after reading.
     */
    @ExperimentalTime
    @RepeatedTest(5)
    fun testPersistence() {
        val data = this.initWithData(random.nextInt(65536))

        /** Close and re-open this DiskManager. */
        this.manager!!.close()
        this.manager = DirectDiskManager(this.path)

        /* Check if data remains the same. */
        this.compareData(data)
    }

    /**
     * Updates [Page]s with random bytes and checks, if those [Page]s' content remains the same after reading.
     */
    @ExperimentalTime
    @RepeatedTest(5)
    fun testUpdatePage() {
        val page = Page(ByteBuffer.allocateDirect(Page.Constants.PAGE_DATA_SIZE_BYTES))
        val data = this.initWithData(random.nextInt(65536))

        val newData = Array(data.size) {
            val bytes = ByteArray(Page.Constants.PAGE_DATA_SIZE_BYTES)
            random.nextBytes(bytes)
            bytes
        }

        /* Update data with new data. */
        for (i in newData.indices) {
            this.manager!!.read((i + 1L), page)

            assertFalse(page.dirty)

            page.putBytes(0, newData[i])

            assertTrue(page.dirty)

            this.manager!!.update(page)
            assertArrayEquals(newData[i], page.getBytes(0))
            assertEquals(i + 1L, page.id)
            assertFalse(page.dirty)
        }

        /* Check if data remains the same. */
        this.compareData(newData)
    }

    /**
     * Compares the data stored in this [DirectDiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareData(ref: Array<ByteArray>) {
        val page = Page(ByteBuffer.allocateDirect(Page.Constants.PAGE_DATA_SIZE_BYTES))
        var readTime = Duration.ZERO
        for (i in ref.indices) {
            readTime += measureTime {
                this.manager!!.read((i + 1L), page)
            }
            assertArrayEquals(ref[i], page.getBytes(0))
            assertEquals(i + 1L, page.id)
            assertFalse(page.dirty)
        }
        println("Reading ${this.manager!!.size `in` Units.MEGABYTE} took $readTime (${(this.manager!!.size `in` Units.MEGABYTE).value / readTime.inSeconds} MB/s).")
    }

    /**
     * Initializes this [DirectDiskManager] with random data.
     *
     * @param size The number of [Page]s to write.
     */
    private fun initWithData(size: Int) : Array<ByteArray> {
        val page = Page(ByteBuffer.allocateDirect(Page.Constants.PAGE_DATA_SIZE_BYTES))
        val data = Array(size) {
            val bytes = ByteArray(Page.Constants.PAGE_DATA_SIZE_BYTES)
            random.nextBytes(bytes)
            bytes
        }

        for (i in data.indices) {
            page.putBytes(0, data[i])

            assertTrue(page.dirty)

            this.manager!!.allocate(page)
            assertEquals(this.manager!!.pages, i+1L)
            assertEquals(((i+2)*Page.Constants.PAGE_DATA_SIZE_BYTES).toDouble(), this.manager!!.size.value)
            assertEquals((i + 1L), page.id)
            assertFalse(page.dirty)
        }

        return data
    }
}