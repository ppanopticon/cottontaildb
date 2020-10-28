package org.vitrivr.cottontail.storage.store.engine.hare.access.variable

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.column.DoubleVectorColumnType
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.storage.basics.Units
import org.vitrivr.cottontail.storage.engine.hare.access.column.variable.VariableHareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.disk.direct.DirectHareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.storage.store.engine.hare.access.AbstractCursorTest
import java.nio.file.Files
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Test case for [VariableHareColumnFile] data structure in combination with [DoubleVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class HareDoubleArrayCursorTest : AbstractCursorTest() {
    /** Path to column file. */
    val path = TestConstants.testDataPath.resolve("test-double-vector-cursor-db.hare")

    /** Seed for random number generator. */
    val seed = System.currentTimeMillis()

    @AfterEach
    fun teardown() {
        Files.delete(this.path)
    }

    /**
     *
     */
    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun test(dimensions: Int) {
        val columnDef = ColumnDef(Name.ColumnName("test"), DoubleVectorColumnType, logicalSize = dimensions)
        VariableHareColumnFile.create(this.path, columnDef)
        val hareFile: VariableHareColumnFile<DoubleVectorValue> = VariableHareColumnFile(this.path, false)

        this.initWithData(hareFile, dimensions, TestConstants.collectionSize)
        this.compareData(hareFile, dimensions, TestConstants.collectionSize)

        /** Cleanup. */
        hareFile.close()
    }

    /**
     * Compares the data stored in this [DirectHareDiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareData(hareFile: VariableHareColumnFile<DoubleVectorValue>, dimensions: Int, size: Int) {
        val cursor = hareFile.cursor()
        val random = SplittableRandom(this.seed)
        val readTime = measureTime {
            while (cursor.next()) {
                val doubleVectorValue = cursor.get()
                Assertions.assertArrayEquals(DoubleVectorValue.random(dimensions, random).data, doubleVectorValue?.data)

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
    private fun initWithData(hareFile: VariableHareColumnFile<DoubleVectorValue>, dimensions: Int, size: Int) {
        var writeTime = Duration.ZERO
        val random = SplittableRandom(this.seed)
        val cursor = hareFile.writableCursor()
        for (d in 0 until size) {
            writeTime += measureTime {
                cursor.append(DoubleVectorValue.random(dimensions, random))
            }
        }
        cursor.close()
        val physSize = (hareFile.bufferPool.diskSize `in` Units.MEGABYTE)
        println("Writing $size doubles vectors (d=$dimensions) to a total of $physSize took $writeTime (${physSize.value / writeTime.inSeconds} MB/s).")
    }
}