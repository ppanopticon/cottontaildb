package org.vitrivr.cottontail.storage.store.engine.hare.access.fixed

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.storage.basics.Units
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnCursor
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnReader
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnWriter
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.disk.direct.DirectHareDiskManager
import java.nio.file.Files
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

class HareDoubleCursorTest {
    val path = TestConstants.testDataPath.resolve("test-double-cursor-db.hare")

    /** [ColumnDef] for test. */
    var columnDef = ColumnDef(Name.ColumnName("test"), Type.Double)

    /** [FixedHareColumnFile] for test. */
    var hareFile: FixedHareColumnFile<DoubleValue>? = null

    /** [BufferPool] for test. */
    var bufferPool: BufferPool? = null

    /** Seed for random number generator. */
    val seed = System.currentTimeMillis()


    @BeforeEach
    fun beforeEach() {
        if (Files.exists(path)) Files.delete(this.path)
        FixedHareColumnFile.createDirect(this.path, this.columnDef)
        val tid = 0L
        this.hareFile = FixedHareColumnFile(this.path)
        this.bufferPool = BufferPool(this.hareFile!!.disk, tid, 25)
    }

    @AfterEach
    fun afterEach() {
        this.hareFile!!.close()
        Files.delete(this.path)
    }

    /**
     * Populates a [FixedHareColumnFile] with [DoubleValue]s and then reads and compares them.
     */
    @Test
    @ExperimentalTime
    fun test() {
        this.initWithData()
        this.compareData()
    }

    /**
     * Initializes this [DirectHareDiskManager] with random data.
     */
    @ExperimentalTime
    private fun initWithData() {
        var writeTime = Duration.ZERO
        val random = SplittableRandom(this.seed)
        FixedHareColumnWriter(this.hareFile!!, this.bufferPool!!).use { writer ->
            var written = 0
            repeat(TestConstants.collectionSize) {
                val d = DoubleValue(random.nextDouble())
                writeTime += measureTime {
                    writer.append(d)
                    written++
                }
            }
            val diskSize = this.hareFile!!.disk.size `in` Units.MEGABYTE
            writer.commit()
            println("Writing $written doubles ($diskSize) took $writeTime (${diskSize.value / writeTime.inSeconds} MB/s).")
        }
    }

    /**
     * Compares the data stored in this [DirectHareDiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareData() {
        val random = SplittableRandom(this.seed)
        val cursor = FixedHareColumnCursor(this.hareFile!!, this.bufferPool!!)
        FixedHareColumnReader(this.hareFile!!, this.bufferPool!!).use { reader ->
            var read = 0L
            val readTime = measureTime {
                for (tupleId in cursor) {
                    val doubleValue = reader.get(tupleId)
                    assertEquals(DoubleValue(random.nextDouble()), doubleValue)
                    read++
                }
                assertEquals(reader.count(), read)
            }
            val diskSize = this.hareFile!!.disk.size `in` Units.MEGABYTE
            println("Reading $read doubles ($diskSize) took $readTime (${diskSize.value / readTime.inSeconds} MB/s).")
        }
    }
}