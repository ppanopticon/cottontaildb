package ch.unibas.dmi.dbis.cottontail.storage.store.engine.hare.access

import ch.unibas.dmi.dbis.cottontail.database.column.DoubleColumnType
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.values.DoubleValue
import ch.unibas.dmi.dbis.cottontail.storage.basics.MemorySize
import ch.unibas.dmi.dbis.cottontail.storage.basics.Units
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column.FixedHareColumn
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.BufferPool
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DirectDiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page
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

    var hare: FixedHareColumn<DoubleValue>? = null

    val random = SplittableRandom(System.currentTimeMillis())

    @BeforeEach
    fun beforeEach() {
        FixedHareColumn.createDirect(this.path, this.columnDef)
        this.hare = FixedHareColumn(this.path, false)
    }

    @AfterEach
    fun afterEach() {
        this.hare!!.close()
        Files.delete(this.path)
    }

    /**
     *
     */
    @Test
    @ExperimentalTime
    fun test() {
        val data = this.initWithData(10_000_000)
        this.compareData(data)
    }

    /**
    * Compares the data stored in this [DirectDiskManager] with the data provided as array of [ByteArray]s
    */
    @ExperimentalTime
    private fun compareData(ref: Array<DoubleValue>) {
        var readTime = Duration.ZERO
        val cursor = this.hare!!.cursor(writeable = false)
        for (r in ref) {
            readTime += measureTime {
                cursor.next()
                assertEquals(r, cursor.get())
            }
        }
        println("Reading ${ref.size} doubles took $readTime (${(MemorySize((this.hare!!.sizePerEntry * ref.size).toDouble(), Units.BYTE) `in` Units.MEGABYTE).value / readTime.inSeconds} MB/s). ")
    }

    /**
     * Initializes this [DirectDiskManager] with random data.
     *
     * @param size The number of [Page]s to write.
     */
    @ExperimentalTime
    private fun initWithData(size: Int) : Array<DoubleValue> {
        var writeTime = Duration.ZERO
        val data = Array(size) {
            DoubleValue(random.nextDouble())
        }

        val cursor = this.hare!!.cursor(writeable = true)
        for (d in data) {
            writeTime += measureTime {
                cursor.append(d)
            }
        }
        cursor.close()
        println("Writing ${data.size} doubles took $writeTime (${(MemorySize((this.hare!!.sizePerEntry * data.size).toDouble(), Units.BYTE) `in` Units.MEGABYTE).value / writeTime.inSeconds} MB/s).")
        return data
    }
}