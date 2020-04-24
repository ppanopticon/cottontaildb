package ch.unibas.dmi.dbis.cottontail.storage.store.engine.hare.access

import ch.unibas.dmi.dbis.cottontail.database.column.FloatVectorColumnType
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.values.FloatVectorValue
import ch.unibas.dmi.dbis.cottontail.storage.basics.Units
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column.FixedHareColumnFile
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DataPage
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DirectDiskManager
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

class HareFloatArrayCursorTest {
    /** Path to column file. */
    val path = Paths.get("./test-float-vector-cursor-db.hare")

    /** Seed for random number generator. */
    val seed = System.currentTimeMillis()

    /**
     *
     */
    @ExperimentalTime
    @ParameterizedTest
    @ValueSource(ints = [2048])
    fun test(dimensions: Int) {
        val columnDef = ColumnDef(Name("test"), FloatVectorColumnType, size = dimensions)
        FixedHareColumnFile.createDirect(this.path, columnDef)
        val hareFile: FixedHareColumnFile<FloatVectorValue> = FixedHareColumnFile(this.path, false)

        this.initWithData(hareFile, dimensions, 1_000_000)
        this.compareData(hareFile, dimensions, 1_000_000)

        /** Cleanup. */
        hareFile.close()
        Files.delete(this.path)
    }

    /**
     * Compares the data stored in this [DirectDiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareData(hareFile: FixedHareColumnFile<FloatVectorValue>, dimensions: Int, size: Int) {
        val cursor = hareFile.cursor(writeable = false)
        val random = SplittableRandom(this.seed)
        val readTime = measureTime {
            cursor.forEach { _, floatVectorValue ->
               //Assertions.assertArrayEquals(FloatVectorValue.random(dimensions, random).data, floatVectorValue?.data)
            }
        }
        cursor.close()
        val physSize = (hareFile.bufferPool.diskSize `in` Units.MEGABYTE)
        println("Reading $size doubles vectors (d=$dimensions) to a total of $physSize took $readTime (${physSize.value / readTime.inSeconds} MB/s).")
    }

    /**
     * Initializes this [DirectDiskManager] with random data.
     *
     * @param size The number of [DataPage]s to write.
     */
    @ExperimentalTime
    private fun initWithData(hareFile: FixedHareColumnFile<FloatVectorValue>, dimensions: Int, size: Int) {
        var writeTime = Duration.ZERO
        val cursor = hareFile.cursor(writeable = true)
        val random = SplittableRandom(this.seed)
        for (d in 0 until size) {
            writeTime += measureTime {
                cursor.append(FloatVectorValue.random(dimensions, random))
            }
        }
        cursor.close()
        val physSize = (hareFile.bufferPool.diskSize `in` Units.MEGABYTE)
        println("Writing $size doubles vectors (d=$dimensions) to a total of $physSize took $writeTime (${physSize.value / writeTime.inSeconds} MB/s).")
    }
}