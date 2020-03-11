package ch.unibas.dmi.dbis.cottontail.storage.store.engine.hare

import ch.unibas.dmi.dbis.cottontail.storage.basics.Units
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.BufferPool
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page
import org.junit.jupiter.api.*
import java.nio.ByteBuffer

import java.nio.file.Files
import java.nio.file.Paths
import java.util.*

import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

class BufferPoolTest {
    val path = Paths.get("./test-bufferpool-db.hare")


    var _manager: DiskManager? = null

    var pool: BufferPool? = null

    val random = SplittableRandom(System.currentTimeMillis())

    @BeforeEach
    fun beforeEach() {
        DiskManager.init(this.path)
        this._manager = DiskManager(this.path)
        this.pool = BufferPool(this._manager!!)
    }

    @AfterEach
    fun afterEach() {
        this._manager!!.close()
        Files.delete(this.path)
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
            val bytes = ByteArray(Page.Constants.PAGE_DATA_SIZE_BYTES)
            random.nextBytes(bytes)
            bytes
        }

        /* Update data with new data. */
        for (i in newData.indices) {
            val page = this.pool!!.get((i + 1L))

            Assertions.assertEquals(i.toLong(), page.id)

            val stamp = page.retain(true)
            page.putBytes(stamp, 0, newData[i])
            page.release(stamp)
        }

        /* Check if data remains the same. */
        this.compareData(newData)
    }

    /**
     * Compares the data stored in this [DiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareData(ref: Array<ByteArray>) {
        var readTime = Duration.ZERO
        for (i in ref.indices) {
            var page: BufferPool.PageRef? = null
            readTime += measureTime {
                page = this.pool!!.get((i + 1L))
            }
            val stamp = page!!.retain(false)

            Assertions.assertArrayEquals(ref[i], page!!.getBytes(stamp,0))
            Assertions.assertEquals(i.toLong(), page!!.id)
            page!!.release(stamp)
        }
        println("Reading ${this._manager!!.size `in` Units.MEGABYTE} took $readTime (${(this._manager!!.size `in` Units.MEGABYTE).value / readTime.inSeconds} MB/s).")
    }

    /**
     * Initializes this [DiskManager] with random data.
     *
     * @param size The number of [Page]s to write.
     */
    private fun initWithData(size: Int) : Array<ByteArray> {
        val data = Array(size) {
            val bytes = ByteArray(Page.Constants.PAGE_DATA_SIZE_BYTES)
            random.nextBytes(bytes)
            bytes
        }

        for (i in data.indices) {
            val page = this.pool!!.append()
            val stamp = page.retain(true)
            page.putBytes(stamp, 0, data[i])
            page.release(stamp)
        }

        /** Flush data to disk. */
        this.pool!!.flush()
        return data
    }
}