package ch.unibas.dmi.dbis.cottontail.storage.store.engine.hare.disk

import ch.unibas.dmi.dbis.cottontail.storage.basics.Units
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.*

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach

import java.nio.file.Paths
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

class DirectDiskManagerTest {
    val path = Paths.get("./test-direct-diskmgr-db.hare")

    var manager: DirectDiskManager? = null

    val random = SplittableRandom(System.currentTimeMillis())

    val pageShift = 12

    val pageSize = 1 shl this.pageShift

    @BeforeEach
    fun beforeEach() {
        DiskManager.create(this.path, pageShift)
        this.manager = DirectDiskManager(path = this.path)
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
        assertEquals(pageSize, this.manager!!.size.value.toInt())
        assertTrue(this.manager!!.validate())
    }

    /**
     * Appends [Page]s of random bytes and checks, if those [Page]s' content remains the same after reading.
     */
    @ExperimentalTime
    @ParameterizedTest(name="DirectDiskManager (Append / Read): pageSize={0}")
    @ValueSource(ints = [5000, 10000, 20000, 50000, 100000])
    fun testAppendPage(size: Int) {
        val data = this.initWithData(size)

        /* Check if data remains the same. */
        this.compareSingleRead(data)
        this.compareMultiRead(data)
    }

    /**
     * Appends [Page]s of random bytes and checks, if those [Page]s' content remains the same after reading.
     */
    @ExperimentalTime
    @ParameterizedTest(name="DirectDiskManager (Append / Close / Read): pageSize={0}")
    @ValueSource(ints = [5000, 10000, 20000, 50000, 100000])
    fun testPersistence(size: Int) {
        val data = this.initWithData(size)

        /** Close and re-open this DiskManager. */
        this.manager!!.close()
        this.manager = DirectDiskManager(this.path)
        assertTrue(this.manager!!.validate())

        /* Check if data remains the same. */
        this.compareSingleRead(data)
        this.compareMultiRead(data)
    }

    /**
     * Updates [Page]s with random bytes and checks, if those [Page]s' content remains the same after reading.
     */
    @ExperimentalTime
    @ParameterizedTest(name="DirectDiskManager (Append / Update / Read): pageSize={0}")
    @ValueSource(ints = [5000, 10000, 20000, 50000, 100000])
    fun testUpdatePage(size: Int) {
        val page = Page(ByteBuffer.allocateDirect(pageSize))
        val data = this.initWithData(size)

        val newData = Array(data.size) {
            val bytes = ByteArray(pageSize)
            random.nextBytes(bytes)
            bytes
        }

        /* Update data with new data. */
        var updateTime = Duration.ZERO
        for (i in newData.indices) {
            updateTime += measureTime {
                page.putBytes(0, newData[i])
                this.manager!!.update((i + 1L), page)
            }
            assertArrayEquals(newData[i], page.getBytes(0))
        }

        val diskSize = this.manager!!.size `in` Units.MEGABYTE
        println("Updating $diskSize took $updateTime (${diskSize.value / updateTime.inSeconds} MB/s).")

        /* Check if data remains the same. */
        this.compareSingleRead(newData)
        this.compareMultiRead(newData)
    }

    /**
     * Compares the data stored in this [DirectDiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareSingleRead(ref: Array<ByteArray>) {
        val page = Page(ByteBuffer.allocateDirect(pageSize))

        var readTime = Duration.ZERO
        for (i in ref.indices) {
            readTime += measureTime {
                this.manager!!.read((i + 1L), page)
            }
            assertArrayEquals(ref[i], page.getBytes(0))
        }
        val diskSize = this.manager!!.size `in` Units.MEGABYTE
        println("Reading $diskSize (single) took $readTime (${diskSize.value / readTime.inSeconds} MB/s).")
    }

    /**
     * Compares the data stored in this [DirectDiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareMultiRead(ref: Array<ByteArray>) {
        val page1 = Page(ByteBuffer.allocateDirect(pageSize))
        val page2 = Page(ByteBuffer.allocateDirect(pageSize))
        val page3 = Page(ByteBuffer.allocateDirect(pageSize))
        val page4 = Page(ByteBuffer.allocateDirect(pageSize))

        var readTime = Duration.ZERO
        for (i in ref.indices step 4) {
            readTime += measureTime {
                this.manager!!.read((i + 1L), arrayOf(page1, page2, page3, page4))
            }
            assertArrayEquals(ref[i], page1.getBytes(0))
            assertArrayEquals(ref[i+1], page2.getBytes(0))
            assertArrayEquals(ref[i+2], page3.getBytes(0))
            assertArrayEquals(ref[i+3], page4.getBytes(0))
        }
        val diskSize = this.manager!!.size `in` Units.MEGABYTE
        println("Reading $diskSize (multi) took $readTime (${diskSize.value / readTime.inSeconds} MB/s).")
    }

    /**
     * Initializes this [DirectDiskManager] with random data.
     *
     * @param size The number of [Page]s to write.
     */
    @ExperimentalTime
    private fun initWithData(size: Int) : Array<ByteArray> {
        val page = Page(ByteBuffer.allocateDirect(pageSize))
        var writeTime = Duration.ZERO
        val data = Array(size) {
            val bytes = ByteArray(pageSize)
            random.nextBytes(bytes)
            bytes
        }

        for (i in data.indices) {
            writeTime += measureTime {
                page.putBytes(0, data[i])
                this.manager!!.allocate(page)
            }
            assertEquals(this.manager!!.pages, i+1L)
        }

        val diskSize = this.manager!!.size `in` Units.MEGABYTE
        println("Appending $diskSize took $writeTime (${diskSize.value / writeTime.inSeconds} MB/s).")
        return data
    }
}