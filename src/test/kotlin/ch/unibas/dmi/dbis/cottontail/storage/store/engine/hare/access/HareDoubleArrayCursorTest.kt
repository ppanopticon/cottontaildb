package ch.unibas.dmi.dbis.cottontail.storage.store.engine.hare.access

import ch.unibas.dmi.dbis.cottontail.database.column.DoubleVectorColumnType
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.values.DoubleVectorValue
import ch.unibas.dmi.dbis.cottontail.storage.basics.Units
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column.FixedHareColumn
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DataPage
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DirectDiskManager
import ch.unibas.dmi.dbis.cottontail.utilities.generators.DoubleVectorGenerator
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

class HareDoubleArrayCursorTest {
    val path = Paths.get("./test-cursor-db.hare")

    val seed = System.currentTimeMillis()

    /**
     *
     */
    @ExperimentalTime
    @ParameterizedTest
    @ValueSource(ints = [2048, 1024, 512])
    fun test(dimensions: Int) {
        val columnDef = ColumnDef(Name("test"), DoubleVectorColumnType, size = dimensions)
        FixedHareColumn.createDirect(this.path, columnDef)
        val hare: FixedHareColumn<DoubleVectorValue> = FixedHareColumn(this.path, false)

        this.initWithData(hare, dimensions, 1_000_000)
        this.compareData(hare, dimensions, 1_000_000)

        /** Cleanup. */
        hare.close()
        Files.delete(this.path)
    }

    /**
     * Compares the data stored in this [DirectDiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareData(hare: FixedHareColumn<DoubleVectorValue>, dimensions: Int, size: Int) {
        var readTime = Duration.ZERO
        val cursor = hare.cursor(writeable = false)
        val generator = DoubleVectorGenerator(dimension = dimensions, random = SplittableRandom(this.seed), items = size)
        var value: DoubleVectorValue? = null

        while (cursor.hasNext()) {
            readTime += measureTime {
                cursor.next()
                value = cursor.get()
            }
            Assertions.assertEquals(0, generator.next().data.rewind().compareTo(value!!.data.rewind()))
        }
        val physSize = (hare.bufferPool.diskSize `in` Units.MEGABYTE)
        println("Reading $size doubles vectors (d=$dimensions) to a total of $physSize took $readTime (${physSize.value / readTime.inSeconds} MB/s).")
    }

    /**
     * Initializes this [DirectDiskManager] with random data.
     *
     * @param size The number of [DataPage]s to write.
     */
    @ExperimentalTime
    private fun initWithData(hare: FixedHareColumn<DoubleVectorValue>, dimensions: Int, size: Int) {
        var writeTime = Duration.ZERO
        val generator = DoubleVectorGenerator(dimension = dimensions, random = SplittableRandom(this.seed), items = size)

        val cursor = hare.cursor(writeable = true)
        for (d in generator) {
            writeTime += measureTime {
                cursor.append(d)
            }
        }
        cursor.close()
        val physSize = (hare.bufferPool.diskSize `in` Units.MEGABYTE)
        println("Writing $size doubles vectors (d=$dimensions) to a total of $physSize took $writeTime (${physSize.value / writeTime.inSeconds} MB/s).")
    }
}