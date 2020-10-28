package org.vitrivr.cottontail.storage.store.engine.hare.disk

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.storage.basics.Units
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.basics.PageConstants
import org.vitrivr.cottontail.storage.engine.hare.disk.HareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.direct.DirectHareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.storage.engine.hare.disk.wal.WALHareDiskManager
import java.nio.ByteBuffer
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

class WALHareDiskManagerTest {
    val path = TestConstants.testDataPath.resolve("test-direct-diskmgr-db.hare")

    var manager: WALHareDiskManager? = null

    val random = SplittableRandom(System.currentTimeMillis())

    val pageShift = 12

    val pageSize = 1 shl this.pageShift

    @BeforeEach
    fun beforeEach() {
        HareDiskManager.create(this.path, this.pageShift)
        this.manager = WALHareDiskManager(this.path)
    }

    @AfterEach
    fun afterEach() {
        this.manager!!.delete()
    }

    @Test
    fun testCreationAndLoading() {
        Assertions.assertEquals(this.path, this.manager!!.path)
        Assertions.assertEquals(0, this.manager!!.pages)
        Assertions.assertEquals(this.manager!!.pageSize, this.manager!!.size.value.toInt())
    }

    /**
     * Appends [HarePage]s of random bytes and checks, if those [HarePage]s' content remains the same after reading.
     */
    @ExperimentalTime
    @ParameterizedTest(name="WALDiskManager (Append / Commit / Read): pageSize={0}")
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
    @ParameterizedTest(name = "WALDiskManager (Append / Commit / Close / Read): pages={0}")
    @ValueSource(ints = [5000, 10000, 20000, 50000, 100000])
    fun testPersistence(size: Int) {
        val data = this.initWithData(size)

        /** Close and re-open this DiskManager. */
        this.manager!!.close()
        this.manager = WALHareDiskManager(this.path)

        /* Check if data remains the same. */
        this.compareSingleRead(data)
        this.compareMultiRead(data)
    }

    /**
     * Frees [HarePage]s and checks correctness of their content, with a commit in between.
     */
    @ExperimentalTime
    @ParameterizedTest(name="DirectDiskManager (Append / Free / Commit / Read): pages={0}")
    @ValueSource(ints = [5000, 10000, 20000, 50000, 100000])
    fun testFreePage(size: Int) {
        val data = this.initWithData(size)
        val random = SplittableRandom()
        val pageIds = mutableListOf<PageId>()

        /* Truncate last page and compare sizes. */
        for (i in 0 until 100) {
            val pageId = random.nextLong(1L, data.size.toLong())
            if (!pageIds.contains(pageId)) {
                this.manager!!.free(pageId)
                pageIds.add(pageId)
            } else {
                Assertions.assertThrows(IllegalArgumentException::class.java) {
                    this.manager!!.free(pageId)
                }
            }
        }
        this.manager!!.commit()

        /* Check page content. */
        val page = HarePage(ByteBuffer.allocateDirect(pageSize))
        for (pageId in pageIds) {
            this.manager!!.read(pageId, page)
            Assertions.assertEquals(PageConstants.PAGE_TYPE_FREED, page.getInt(0))
        }
    }

    /**
     * Frees [HarePage]s and checks correctness of page reuse with commit in between.
     */
    @ExperimentalTime
    @ParameterizedTest(name="DirectDiskManager (Append / Free / Commit / Append): pages={0}")
    @ValueSource(ints = [5000, 10000, 20000, 50000, 100000])
    fun testFreePageReuse(size: Int) {
        val data = this.initWithData(size)
        val random = SplittableRandom()
        val pageIds = mutableListOf<PageId>()

        /* Free random pages. */
        for (i in 0 until 100) {
            val pageId = random.nextLong(1L, data.size.toLong())
            if (!pageIds.contains(pageId)) {
                this.manager!!.free(pageId)
                pageIds.add(pageId)
            } else {
                Assertions.assertThrows(IllegalArgumentException::class.java) {
                    this.manager!!.free(pageId)
                }
            }
        }
        this.manager!!.commit()

        /* Check page re-use. */
        for (i in pageIds.size-1 downTo 0) {
            Assertions.assertEquals(pageIds[i], this.manager!!.allocate())
        }
        this.manager!!.rollback()
    }

    /**
     * Updates [HarePage]s with random bytes and checks, if those [HarePage]s' content remains the same after reading.
     */
    @ExperimentalTime
    @ParameterizedTest(name = "WALDiskManager (Append / Commit / Update / Commit / Read): pages={0}")
    @ValueSource(ints = [5000, 10000, 20000, 50000, 100000])
    fun testUpdateWithCommit(size: Int) {
        val page = HarePage(ByteBuffer.allocateDirect(this.manager!!.pageSize))
        val data = this.initWithData(size)

        val newData = Array(data.size) {
            val bytes = ByteArray(this.manager!!.pageSize)
            random.nextBytes(bytes)
            bytes
        }

        /* Update data with new data. */
        for (i in newData.indices) {
            this.manager!!.read((i + 1L), page)

            page.putBytes(0, newData[i])

            this.manager!!.update((i + 1L), page)
            Assertions.assertArrayEquals(newData[i], page.getBytes(0))
        }

        this.manager!!.commit()

        /* Check if data remains the same. */
        this.compareSingleRead(newData)
        this.compareMultiRead(newData)
    }

    /**
     * Appends [HarePage]s of random bytes and checks, if those [HarePage]s' content remains the same after reading.
     */
    @ExperimentalTime
    @ParameterizedTest(name = "WALDiskManager (Append / Commit / Update / Rollback / Read): pages={0}")
    @ValueSource(ints = [5000, 10000, 20000, 50000, 100000])
    fun testUpdateWithRollback(size: Int) {
        val page = HarePage(ByteBuffer.allocateDirect(this.manager!!.pageSize))
        val data = this.initWithData(size)

        val newData = Array(data.size) {
            val bytes = ByteArray(this.manager!!.pageSize)
            random.nextBytes(bytes)
            bytes
        }

        /* Update data with new data. */
        for (i in newData.indices) {
            this.manager!!.read((i + 1L), page)


            page.putBytes(0, newData[i])

            this.manager!!.update((i + 1L), page)
            Assertions.assertArrayEquals(newData[i], page.getBytes(0))
        }

        this.manager!!.rollback()

        /* Check if data remains the same. */
        this.compareSingleRead(data)
        this.compareMultiRead(data)
    }

    /**
     * Compares the data stored in this [DirectHareDiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareSingleRead(ref: Array<ByteArray>) {
        val page = HarePage(ByteBuffer.allocateDirect(this.manager!!.pageSize))
        var readTime = Duration.ZERO
        for (i in ref.indices) {
            readTime += measureTime {
                this.manager!!.read((i + 1L), page)
            }
            Assertions.assertArrayEquals(ref[i], page.getBytes(0))
        }
        val diskSize = this.manager!!.size `in` Units.MEGABYTE
        println("Reading $diskSize (single) took $readTime (${diskSize.value / readTime.inSeconds} MB/s).")    }

    /**
     * Compares the data stored in this [DirectHareDiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareMultiRead(ref: Array<ByteArray>) {
        val page1 = HarePage(ByteBuffer.allocateDirect(this.manager!!.pageSize))
        val page2 = HarePage(ByteBuffer.allocateDirect(this.manager!!.pageSize))
        val page3 = HarePage(ByteBuffer.allocateDirect(this.manager!!.pageSize))
        val page4 = HarePage(ByteBuffer.allocateDirect(this.manager!!.pageSize))

        var readTime = Duration.ZERO
        for (i in ref.indices step 4) {
            readTime += measureTime {
                this.manager!!.read((i + 1L), arrayOf(page1, page2, page3, page4))
            }
            Assertions.assertArrayEquals(ref[i], page1.getBytes(0))
            Assertions.assertArrayEquals(ref[i + 1], page2.getBytes(0))
            Assertions.assertArrayEquals(ref[i + 2], page3.getBytes(0))
            Assertions.assertArrayEquals(ref[i + 3], page4.getBytes(0))
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
        val page = HarePage(ByteBuffer.allocateDirect(this.manager!!.pageSize))
        var writeTime = Duration.ZERO

        val data = Array(size) {
            val bytes = ByteArray(this.manager!!.pageSize)
            random.nextBytes(bytes)
            bytes
        }

        var prev: PageId = 0L
        for (i in data.indices) {
            page.putBytes(0, data[i])
            writeTime += measureTime {
                val pageId = this.manager!!.allocate()
                Assertions.assertEquals(prev + 1L, pageId)  /* Make sure pageIds increase monotonically. */
                prev = pageId
                this.manager!!.update(pageId, page)
            }
        }

        /* Commit changes. */
        writeTime += measureTime {
            this.manager!!.commit()
        }

        println("Writing ${this.manager!!.size `in` Units.MEGABYTE} took $writeTime (${(this.manager!!.size `in` Units.MEGABYTE).value / writeTime.inSeconds} MB/s).")

        return data
    }
}