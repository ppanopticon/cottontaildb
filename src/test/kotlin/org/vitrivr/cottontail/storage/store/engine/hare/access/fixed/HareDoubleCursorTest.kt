package org.vitrivr.cottontail.storage.store.engine.hare.access.fixed

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.column.DoubleColumnType
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.storage.basics.Units
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnCursor
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnReader
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnWriter
import org.vitrivr.cottontail.storage.engine.hare.disk.direct.DirectHareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import java.nio.file.Files
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

class HareDoubleCursorTest {
    val path = TestConstants.testDataPath.resolve("test-double-cursor-db.hare")

    var columnDef = ColumnDef(Name.ColumnName("test"), DoubleColumnType)

    var hareFile: FixedHareColumnFile<DoubleValue>? = null


    @BeforeEach
    fun beforeEach() {
        FixedHareColumnFile.createDirect(this.path, this.columnDef)
        this.hareFile = FixedHareColumnFile(this.path, false)
    }

    @AfterEach
    fun afterEach() {
        this.hareFile!!.close()
        Files.delete(this.path)
    }

    /**
     *
     */
    @Test
    @ExperimentalTime
    fun test() {
        val seed = System.currentTimeMillis()
        this.initWithData(TestConstants.collectionSize, seed)
        this.compareData(seed)
    }

    /**
    * Compares the data stored in this [DirectHareDiskManager] with the data provided as array of [ByteArray]s
    */
    @ExperimentalTime
    private fun compareData(seed: Long) {
        val random = SplittableRandom(seed)
        val cursor = FixedHareColumnCursor(this.hareFile!!)
        val reader = FixedHareColumnReader(this.hareFile!!)

        var read = 0
        val readTime = measureTime {
            for (tupleId in cursor) {
                val doubleValue = reader.get(tupleId)
                assertEquals(DoubleValue(random.nextDouble()), doubleValue)
                read++
            }
        }
        val diskSize = this.hareFile!!.disk.size `in` Units.MEGABYTE
        println("Reading $read doubles ($diskSize) took $readTime (${diskSize.value / readTime.inSeconds} MB/s).")
    }

    /**
     * Initializes this [DirectHareDiskManager] with random data.
     *
     * @param size The number of [HarePage]s to write.
     */
    @ExperimentalTime
    private fun initWithData(size: Int, seed: Long) {
        var writeTime = Duration.ZERO
        val random = SplittableRandom(seed)
        val writer = FixedHareColumnWriter(this.hareFile!!)
        var written = 0
        repeat(size) {
            val d = DoubleValue(random.nextDouble())
            writeTime += measureTime {
                writer.append(d)
                written++
            }
        }
        val diskSize = this.hareFile!!.disk.size `in` Units.MEGABYTE
        println("Writing $written doubles ($diskSize) took $writeTime (${diskSize.value / writeTime.inSeconds} MB/s).")
    }
}