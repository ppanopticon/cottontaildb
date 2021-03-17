package org.vitrivr.cottontail.storage.store.engine.hare.buffer

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.storage.basics.Units
import org.vitrivr.cottontail.storage.engine.hare.basics.PageRef
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.disk.HareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.direct.DirectHareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import java.nio.file.Files
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

class BufferPoolTest {
    val path = TestConstants.testDataPath.resolve("test-bufferpool-db.hare")

    var _manager: DirectHareDiskManager? = null

    var pool: BufferPool? = null

    val random = SplittableRandom(System.currentTimeMillis())

    val pageShift = 12

    @BeforeEach
    fun beforeEach() {
        HareDiskManager.create(this.path, this.pageShift)
        val tid = 1L
        this._manager = DirectHareDiskManager(path = this.path, preAllocatePages = 1)
        this.pool = BufferPool(this._manager!!, tid, 10)
    }

    @AfterEach
    fun afterEach() {
        Files.delete(this.path)
    }

    /**
     * Appends [HarePage]s of random bytes and checks, if those [HarePage]s' content remains the same after reading.
     */
    @ExperimentalTime
    @ParameterizedTest(name="BufferPool (direct) (page retention): pages={0}")
    @ValueSource(ints = [5000, 10000, 20000, 50000, 100000])
    fun testPageRetention(pages: Int) {
        val data = this.initWithData(pages)
        val page = this.pool!!.get(1L)
        for (i in 1L until data.size) {
            assertTrue(page === this.pool!!.get(1L))
            this.pool!!.get(i)
        }
    }

    /**
     * Appends [HarePage]s of random bytes and checks, if those [HarePage]s' content remains the same after reading.
     */
    @ExperimentalTime
    @ParameterizedTest(name="BufferPool (direct) (append): pages={0}")
    @ValueSource(ints = [5000, 10000, 20000, 50000, 100000])
    fun testAppendPage(pages: Int) {
        val data = this.initWithData(pages)

        /* Check if data remains the same. */
        this.compareData(data)
    }

    /**
     * Updates [HarePage]s with random bytes and checks, if those [HarePage]s' content remains the same after reading.
     */
    @ExperimentalTime
    @ParameterizedTest(name="BufferPool (direct) (append / update): pages={0}")
    @ValueSource(ints = [5000, 10000, 20000, 50000, 100000])
    fun testUpdatePage(pages: Int) {
        val data = this.initWithData(random.nextInt(65536))

        val newData = Array(data.size) {
            val bytes = ByteArray(this._manager!!.pageSize)
            random.nextBytes(bytes)
            bytes
        }

        /* Update data with new data. */
        var updateTime = Duration.ZERO
        for (i in newData.indices) {
            updateTime += measureTime {
                val page = this.pool!!.get(i + 1L)
                page.putBytes(0, newData[i])
                Assertions.assertEquals(i + 1L, page.id)
            }
        }

        updateTime += measureTime {
            this.pool!!.flush()
        }

        val diskSize = this.pool!!.disk.size `in` Units.MEGABYTE
        println("Updating $diskSize took $updateTime (${diskSize.value / updateTime.inSeconds} MB/s).")

        /* Check if data remains the same. */
        this.compareData(newData)
    }

    /**
     * Compares the data stored in this [DirectHareDiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareData(ref: Array<ByteArray>) {
        var readTime = Duration.ZERO
        for (i in ref.indices) {
            var page: PageRef?
            readTime += measureTime {
                page = this.pool!!.get(i + 1L)
            }

            Assertions.assertArrayEquals(ref[i], page!!.getBytes(0))
            Assertions.assertEquals(i + 1L, page!!.id)
        }
        val diskSize = this.pool!!.disk.size `in` Units.MEGABYTE
        println("Reading $diskSize took $readTime (${diskSize.value / readTime.inSeconds} MB/s).")
    }

    /**
     * Initializes this [DirectHareDiskManager] with random data.
     *
     * @param size The number of [HarePage]s to write.
     */
    @ExperimentalTime
    private fun initWithData(size: Int) : Array<ByteArray> {
        var writeTime = Duration.ZERO
        val data = Array(size) {
            val bytes = ByteArray(this._manager!!.pageSize)
            random.nextBytes(bytes)
            bytes
        }
        writeTime += measureTime {
            for (i in data.indices) {
                val page = this.pool!!.get(this.pool!!.append())
                page.putBytes(0, data[i])
            }
        }

        /** Flush data to disk. */
        writeTime += measureTime {
            this.pool!!.flush()
        }

        val diskSize = this.pool!!.disk.size `in` Units.MEGABYTE
        println("Appending $diskSize took $writeTime (${diskSize.value / writeTime.inSeconds} MB/s).")

        return data
    }
}