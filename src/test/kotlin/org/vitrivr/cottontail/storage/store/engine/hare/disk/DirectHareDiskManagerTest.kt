package org.vitrivr.cottontail.storage.store.engine.hare.disk

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.storage.basics.Units
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.TransactionId
import org.vitrivr.cottontail.storage.engine.hare.basics.PageConstants
import org.vitrivr.cottontail.storage.engine.hare.disk.HareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.direct.DirectHareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

class DirectHareDiskManagerTest {
    val path = TestConstants.testDataPath.resolve("test-direct-diskmgr-db.hare")

    var manager: DirectHareDiskManager? = null

    val random = SplittableRandom(System.currentTimeMillis())

    val pageShift = 12

    val pageSize = 1 shl this.pageShift

    @BeforeEach
    fun beforeEach() {
        HareDiskManager.create(this.path, pageShift)
        this.manager = DirectHareDiskManager(path = this.path, preAllocatePages = 0)
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
        assertEquals(this.manager!!.pageSize, this.manager!!.size.value.toInt())
        assertTrue(this.manager!!.validate())
    }

    /**
     * Appends [HarePage]s of random bytes and checks, if those [HarePage]s' content remains the same after reading.
     */
    @ExperimentalTime
    @ParameterizedTest(name="DirectDiskManager (Append / Read): pages={0}")
    @ValueSource(ints = [5000, 10000, 20000, 50000, 100000])
    fun testAppendPage(size: Int) {
        val data = this.initWithData(size)

        /* Check if data remains the same. */
        this.compareSingleRead(data)
        this.compareMultiRead(data)
    }

    /**
     * Appends [HarePage]s of random bytes and checks, if those [HarePage]s' content remains the same after reading.
     */
    @ExperimentalTime
    @ParameterizedTest(name="DirectDiskManager (Append / Close / Read): pages={0}")
    @ValueSource(ints = [5000, 10000, 20000, 50000, 100000])
    fun testPersistence(size: Int) {
        val data = this.initWithData(size)

        /** Close and re-open this DiskManager. */
        this.manager!!.close()
        this.manager = DirectHareDiskManager(this.path)
        assertTrue(this.manager!!.validate())

        /* Check if data remains the same. */
        this.compareSingleRead(data)
        this.compareMultiRead(data)
    }

    /**
     * Updates [HarePage]s with random bytes and checks, if those [HarePage]s' content remains the same after reading.
     */
    @ExperimentalTime
    @ParameterizedTest(name="DirectDiskManager (Append / Update / Read): pages={0}")
    @ValueSource(ints = [5000, 10000, 20000, 50000, 100000])
    fun testUpdatePage(size: Int) {
        val page = HarePage(ByteBuffer.allocateDirect(pageSize))
        val data = this.initWithData(size)
        val tid: TransactionId = UUID.randomUUID()

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
                this.manager!!.update(tid, i + 1L, page)
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
     * Frees [HarePage]s and checks correctness of their content.
     */
    @ExperimentalTime
    @ParameterizedTest(name="DirectDiskManager (Append / Free / Read): pages={0}")
    @ValueSource(ints = [5000, 10000, 20000, 50000, 100000])
    fun testFreePage(size: Int) {
        val data = this.initWithData(size)
        val random = SplittableRandom()
        val pageIds = mutableListOf<PageId>()
        val tid: TransactionId = UUID.randomUUID()

        /* Truncate last page and compare sizes. */
        for (i in 0 until 100) {
            val pageId = random.nextLong(1L, data.size.toLong())
            if (!pageIds.contains(pageId)) {
                this.manager!!.free(tid, pageId)
                pageIds.add(pageId)
            } else {
                assertThrows(IllegalArgumentException::class.java) {
                    this.manager!!.free(tid, pageId)
                }
            }
        }

        /* Check page content. */
        val page = HarePage(ByteBuffer.allocateDirect(pageSize))
        for (pageId in pageIds) {
            this.manager!!.read(tid, pageId, page)
            assertEquals(PageConstants.PAGE_TYPE_FREED, page.getInt(0))
        }
    }

    /**
     * Frees [HarePage]s and checks correctness of page reuse.
     */
    @ExperimentalTime
    @ParameterizedTest(name="DirectDiskManager (Append / Free / Append): pages={0}")
    @ValueSource(ints = [5000, 10000, 20000, 50000, 100000])
    fun testFreePageReuse(size: Int) {
        val data = this.initWithData(size)
        val random = SplittableRandom()
        val pageIds = mutableListOf<PageId>()
        val tid: TransactionId = UUID.randomUUID()

        /* Truncate last page and compare sizes. */
        for (i in 0 until 100) {
            val pageId = random.nextLong(1L, data.size.toLong())
            if (!pageIds.contains(pageId)) {
                this.manager!!.free(tid, pageId)
                pageIds.add(pageId)
            } else {
                assertThrows(IllegalArgumentException::class.java) {
                    this.manager!!.free(tid, pageId)
                }
            }
        }

        /* Check page re-use. */
        for (i in pageIds.size-1 downTo 0) {
            assertEquals(pageIds[i], this.manager!!.allocate(tid))
        }
    }

    /**
     * Compares the data stored in this [DirectHareDiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareSingleRead(ref: Array<ByteArray>) {
        val page = HarePage(ByteBuffer.allocateDirect(pageSize))
        val tid: TransactionId = UUID.randomUUID()

        var readTime = Duration.ZERO
        for (i in ref.indices) {
            readTime += measureTime {
                this.manager!!.read(tid, i + 1L, page)
            }
            assertArrayEquals(ref[i], page.getBytes(0))
        }
        val diskSize = this.manager!!.size `in` Units.MEGABYTE
        println("Reading $diskSize (single) took $readTime (${diskSize.value / readTime.inSeconds} MB/s).")
    }

    /**
     * Compares the data stored in this [DirectHareDiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareMultiRead(ref: Array<ByteArray>) {
        val page1 = HarePage(ByteBuffer.allocateDirect(pageSize))
        val page2 = HarePage(ByteBuffer.allocateDirect(pageSize))
        val page3 = HarePage(ByteBuffer.allocateDirect(pageSize))
        val page4 = HarePage(ByteBuffer.allocateDirect(pageSize))
        val tid: TransactionId = UUID.randomUUID()

        var readTime = Duration.ZERO
        for (i in ref.indices step 4) {
            readTime += measureTime {
                this.manager!!.read(tid, i + 1L, arrayOf(page1, page2, page3, page4))
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
     * Initializes this [DirectHareDiskManager] with random data.
     *
     * @param size The number of [HarePage]s to write.
     */
    @ExperimentalTime
    private fun initWithData(size: Int) : Array<ByteArray> {
        val page = HarePage(ByteBuffer.allocateDirect(pageSize))
        val tid: TransactionId = UUID.randomUUID()

        var writeTime = Duration.ZERO
        val data = Array(size) {
            val bytes = ByteArray(pageSize)
            random.nextBytes(bytes)
            bytes
        }

        var prev: PageId = 0L
        for (i in data.indices) {
            writeTime += measureTime {
                page.putBytes(0, data[i])
                val pageId = this.manager!!.allocate(tid)
                assertEquals(prev + 1L, pageId) /* Make sure pageIds increase monotonically. */
                prev = pageId
                this.manager!!.update(tid, pageId, page)
            }
            assertEquals(i + 1L, this.manager!!.pages)
        }

        /* Commit changes. */
        this.manager!!.commit(tid)

        val diskSize = this.manager!!.size `in` Units.MEGABYTE
        println("Appending $diskSize took $writeTime (${diskSize.value / writeTime.inSeconds} MB/s).")
        return data
    }
}