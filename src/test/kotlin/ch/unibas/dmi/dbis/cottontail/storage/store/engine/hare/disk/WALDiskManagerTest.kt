package ch.unibas.dmi.dbis.cottontail.storage.store.engine.hare.disk

import ch.unibas.dmi.dbis.cottontail.storage.basics.Units
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Constants.PAGE_DATA_SIZE_BYTES
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DirectDiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.wal.WALDiskManager
import org.junit.jupiter.api.*

import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util.*

import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

class WALDiskManagerTest {
    val path = Paths.get("./test-wal-diskmgr-db.hare")

    var manager: WALDiskManager? = null

    val random = SplittableRandom(System.currentTimeMillis())

    @BeforeEach
    fun beforeEach() {
        DiskManager.create(this.path)
        this.manager = WALDiskManager(this.path)
    }

    @AfterEach
    fun afterEach() {
        this.manager!!.delete()
    }

    @Test
    fun testCreationAndLoading() {
        Assertions.assertEquals(this.path, this.manager!!.path)
        Assertions.assertEquals(0, this.manager!!.pages)
        Assertions.assertEquals(PAGE_DATA_SIZE_BYTES, this.manager!!.size.value.toInt())
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
        this.manager = WALDiskManager(this.path)

        /* Check if data remains the same. */
        this.compareData(data)
    }

    /**
     * Updates [Page]s with random bytes and checks, if those [Page]s' content remains the same after reading.
     */
    @ExperimentalTime
    @RepeatedTest(5)
    fun testUpdateWithCommit() {
        val page = Page(ByteBuffer.allocateDirect(PAGE_DATA_SIZE_BYTES))
        val data = this.initWithData(random.nextInt(65536))

        val newData = Array(data.size) {
            val bytes = ByteArray(PAGE_DATA_SIZE_BYTES)
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
        this.compareData(newData)
    }

    /**
     * Appends [Page]s of random bytes and checks, if those [Page]s' content remains the same after reading.
     */
    @ExperimentalTime
    @RepeatedTest(5)
    fun testUpadateWithRollback() {
        val page = Page(ByteBuffer.allocateDirect(PAGE_DATA_SIZE_BYTES))
        val data = this.initWithData(random.nextInt(65536))

        val newData = Array(data.size) {
            val bytes = ByteArray(PAGE_DATA_SIZE_BYTES)
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
        this.compareData(data)
    }

    /**
     * Compares the data stored in this [DirectDiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareData(ref: Array<ByteArray>) {
        val page = Page(ByteBuffer.allocateDirect(PAGE_DATA_SIZE_BYTES))
        var readTime = Duration.ZERO
        for (i in ref.indices) {
            readTime += measureTime {
                this.manager!!.read((i + 1L), page)
            }
            Assertions.assertArrayEquals(ref[i], page.getBytes(0))
        }
        println("Reading ${this.manager!!.size `in` Units.MEGABYTE} took $readTime (${(this.manager!!.size `in` Units.MEGABYTE).value / readTime.inSeconds} MB/s).")
    }

    /**
     * Initializes this [DirectDiskManager] with random data.
     *
     * @param size The number of [Page]s to write.
     */
    @ExperimentalTime
    private fun initWithData(size: Int) : Array<ByteArray> {
        val page = Page(ByteBuffer.allocateDirect(PAGE_DATA_SIZE_BYTES))
        var writeTime = Duration.ZERO

        val data = Array(size) {
            val bytes = ByteArray(PAGE_DATA_SIZE_BYTES)
            random.nextBytes(bytes)
            bytes
        }

        for (i in data.indices) {
            page.putBytes(0, data[i])
            writeTime += measureTime {
                this.manager!!.allocate(page)
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