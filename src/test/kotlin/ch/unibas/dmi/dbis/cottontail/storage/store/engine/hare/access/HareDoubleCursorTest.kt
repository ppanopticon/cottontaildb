package ch.unibas.dmi.dbis.cottontail.storage.store.engine.hare.access

import ch.unibas.dmi.dbis.cottontail.database.column.DoubleColumnType
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.values.DoubleValue
import ch.unibas.dmi.dbis.cottontail.storage.basics.Units
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column.FixedHareColumnFile
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DataPage
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DirectDiskManager
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

class HareDoubleCursorTest {
    val path = Paths.get("./test-cursor-db.hare")

    var columnDef = ColumnDef(Name("test"), DoubleColumnType)

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
        this.initWithData(10_000_000, seed)
        this.compareData(seed)
    }

    /**
    * Compares the data stored in this [DirectDiskManager] with the data provided as array of [ByteArray]s
    */
    @ExperimentalTime
    private fun compareData(seed: Long) {
        val random = SplittableRandom(seed)
        val cursor = this.hareFile!!.cursor(writeable = false)
        var size = 0
        val readTime = measureTime {
            cursor.forEach { _, doubleValue ->
                assertEquals(DoubleValue(random.nextDouble()), doubleValue)
                size++
            }
        }
        val diskSize = this.hareFile!!.disk.size `in` Units.MEGABYTE
        println("Reading $size doubles ($diskSize) took $readTime (${diskSize.value / readTime.inSeconds} MB/s).")
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
        val cursor = this.hareFile!!.cursor(writeable = true)
        repeat(size) {
            val d = DoubleValue(random.nextDouble())
            writeTime += measureTime {
                cursor.append(d)
            }
        }

        cursor.close()
        val diskSize = this.hareFile!!.disk.size `in` Units.MEGABYTE
        println("Writing $size doubles ($diskSize) took $writeTime (${diskSize.value / writeTime.inSeconds} MB/s).")
    }
}