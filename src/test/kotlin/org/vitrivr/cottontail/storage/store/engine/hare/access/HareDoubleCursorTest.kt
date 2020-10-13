package org.vitrivr.cottontail.storage.store.engine.hare.access

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.column.DoubleColumnType
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.storage.basics.Units
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.disk.DataPage
import org.vitrivr.cottontail.storage.engine.hare.disk.DirectDiskManager
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
        //this.hareFile!!.close()
        //Files.delete(this.path)
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
    * Compares the data stored in this [DirectDiskManager] with the data provided as array of [ByteArray]s
    */
    @ExperimentalTime
    private fun compareData(seed: Long) {
        val random = SplittableRandom(seed)
        val cursor = this.hareFile!!.cursor()
        var read = 0

        val action: ((TupleId, DoubleValue?) -> Unit) = { _, doubleValue ->

        }

        val readTime = measureTime {
            while (cursor.next()) {
                val doubleValue = cursor.get()
                assertEquals(DoubleValue(random.nextDouble()), doubleValue)
                read++
            }
        }
        val diskSize = this.hareFile!!.disk.size `in` Units.MEGABYTE
        println("Reading $read doubles ($diskSize) took $readTime (${diskSize.value / readTime.inSeconds} MB/s).")
        cursor.close()
    }

    /**
     * Initializes this [DirectDiskManager] with random data.
     *
     * @param size The number of [DataPage]s to write.
     */
    @ExperimentalTime
    private fun initWithData(size: Int, seed: Long) {
        var writeTime = Duration.ZERO
        val random = SplittableRandom(seed)
        val cursor = this.hareFile!!.writableCursor()
        var written = 0
        repeat(size) {
            val d = DoubleValue(random.nextDouble())
            writeTime += measureTime {
                cursor.append(d)
                written++
            }
        }

        cursor.close()
        val diskSize = this.hareFile!!.disk.size `in` Units.MEGABYTE
        println("Writing $written doubles ($diskSize) took $writeTime (${diskSize.value / writeTime.inSeconds} MB/s).")
    }
}