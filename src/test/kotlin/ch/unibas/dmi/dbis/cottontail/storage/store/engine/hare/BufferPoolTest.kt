package ch.unibas.dmi.dbis.cottontail.storage.store.engine.hare

import ch.unibas.dmi.dbis.cottontail.storage.basics.Units
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.BufferPool
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.Priority
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Constants.PAGE_DATA_SIZE_BYTES
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DirectDiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertTrue

import java.nio.file.Files
import java.nio.file.Paths
import java.util.*

import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

class BufferPoolTest {
    val path = Paths.get("./test-bufferpool-db.hare")

    var _manager: DirectDiskManager? = null

    var pool: BufferPool? = null

    val random = SplittableRandom(System.currentTimeMillis())

    @BeforeEach
    fun beforeEach() {
        DiskManager.create(this.path)
        this._manager = DirectDiskManager(this.path)
        this.pool = BufferPool(this._manager!!)
    }

    @AfterEach
    fun afterEach() {
        this.pool!!.close()
        Files.delete(this.path)
    }

    /**
     * Appends [Page]s of random bytes and checks, if those [Page]s' content remains the same after reading.
     */
    @ExperimentalTime
    @RepeatedTest(5)
    fun testPageRetention() {
        val data = this.initWithData(random.nextInt(65536))
        val page = this.pool!!.get(1L, Priority.HIGH)
        page.release()
        for (i in 1L until data.size) {
            assertTrue(page === this.pool!!.get(1L))
            this.pool!!.get(i, Priority.DEFAULT).release()
        }
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
     * Updates [Page]s with random bytes and checks, if those [Page]s' content remains the same after reading.
     */
    @ExperimentalTime
    @RepeatedTest(5)
    fun testUpdatePage() {
        val data = this.initWithData(random.nextInt(65536))

        val newData = Array(data.size) {
            val bytes = ByteArray(PAGE_DATA_SIZE_BYTES)
            random.nextBytes(bytes)
            bytes
        }

        /* Update data with new data. */
        for (i in newData.indices) {
            val page = this.pool!!.get((i + 1L))

            Assertions.assertEquals((i + 1L), page.id)

            page.putBytes(0, newData[i])
            page.release()
        }
        this.pool!!.flush()

        /* Check if data remains the same. */
        this.compareData(newData)
    }

    /**
     * Compares the data stored in this [DirectDiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareData(ref: Array<ByteArray>) {
        var readTime = Duration.ZERO
        for (i in ref.indices) {
            var page: BufferPool.PageRef?
            readTime += measureTime {
                page = this.pool!!.get((i + 1L))
            }

            Assertions.assertArrayEquals(ref[i], page!!.getBytes(0))
            Assertions.assertEquals((i + 1L), page!!.id)
            page!!.release()
        }
        println("Reading ${this._manager!!.size `in` Units.MEGABYTE} took $readTime (${(this._manager!!.size `in` Units.MEGABYTE).value / readTime.inSeconds} MB/s).")
    }

    /**
     * Initializes this [DirectDiskManager] with random data.
     *
     * @param size The number of [Page]s to write.
     */
    private fun initWithData(size: Int) : Array<ByteArray> {
        val data = Array(size) {
            val bytes = ByteArray(PAGE_DATA_SIZE_BYTES)
            random.nextBytes(bytes)
            bytes
        }

        for (i in data.indices) {
            val page = this.pool!!.append()
            page.putBytes(0, data[i])
            page.release()
        }

        /** Flush data to disk. */
        this.pool!!.flush()
        return data
    }
}