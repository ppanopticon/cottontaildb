package org.vitrivr.cottontail.storage.store.engine.hare.access.fixed

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.column.FloatVectorColumnType
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.FloatVectorValue
import org.vitrivr.cottontail.storage.basics.Units
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.disk.direct.DirectHareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.storage.store.engine.hare.access.AbstractCursorTest
import java.nio.file.Files
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

class HareFloatArrayCursorTest : AbstractCursorTest() {
    /** Path to column file. */
    val path = TestConstants.testDataPath.resolve("test-float-vector-cursor-db.hare")

    /** Seed for random number generator. */
    val seed = System.currentTimeMillis()

    /**
     *
     */
    @ExperimentalTime
    @ParameterizedTest
    @ValueSource(ints = [2048])
    fun test(dimensions: Int) {
        val columnDef = ColumnDef(Name.ColumnName("test"), FloatVectorColumnType, logicalSize = dimensions)
        FixedHareColumnFile.createDirect(this.path, columnDef)
        val hareFile: FixedHareColumnFile<FloatVectorValue> = FixedHareColumnFile(this.path, false)

        this.initWithData(hareFile, dimensions, TestConstants.collectionSize)
        this.compareData(hareFile, dimensions, TestConstants.collectionSize)

        /** Cleanup. */
        hareFile.close()
        Files.delete(this.path)
    }

    /**
     * Compares the data stored in this [DirectHareDiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareData(hareFile: FixedHareColumnFile<FloatVectorValue>, dimensions: Int, size: Int) {
        val cursor = hareFile.cursor()
        val query = FloatVectorValue.random(dimensions)
        val knn = MinHeapSelection<ComparablePair<TupleId, Double>>(500)
        val random = SplittableRandom(this.seed)
        val readTime = measureTime {
            while (cursor.next()) {
                val floatVectorValue = cursor.get()
                Assertions.assertArrayEquals(FloatVectorValue.random(dimensions, random).data, floatVectorValue?.data)
            }
        }
        cursor.close()
        val physSize = (hareFile.bufferPool.diskSize `in` Units.MEGABYTE)
        println("Reading $size doubles vectors (d=$dimensions) to a total of $physSize took $readTime (${physSize.value / readTime.inSeconds} MB/s).")
    }

    /**
     * Initializes this [DirectHareDiskManager] with random data.
     *
     * @param size The number of [HarePage]s to write.
     */
    @ExperimentalTime
    private fun initWithData(hareFile: FixedHareColumnFile<FloatVectorValue>, dimensions: Int, size: Int) {
        var writeTime = Duration.ZERO
        val cursor = hareFile.writableCursor()
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