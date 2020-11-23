package org.vitrivr.cottontail.storage.store.engine.hare.access.fixed

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
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnCursor
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnReader
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnWriter
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.buffer.eviction.EvictionPolicy
import org.vitrivr.cottontail.storage.engine.hare.disk.direct.DirectHareDiskManager
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.storage.store.engine.hare.access.AbstractCursorTest
import java.nio.file.Files
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

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
     * Populates a [FixedHareColumnFile] with [DoubleVectorValue]s and then reads and compares them.
     */
    @ExperimentalTime
    @ParameterizedTest
    @MethodSource("dimensions")
    fun test(dimensions: Int) {
        val columnDef = ColumnDef(Name.ColumnName("test"), DoubleVectorColumnType, logicalSize = dimensions)
        FixedHareColumnFile.createDirect(this.path, columnDef)
        val tid = UUID.randomUUID()
        val hareFile: FixedHareColumnFile<DoubleVectorValue> = FixedHareColumnFile(this.path)
        val bufferPool = BufferPool(hareFile.disk, tid, 25, EvictionPolicy.LRU)

        this.initWithData(hareFile, bufferPool, dimensions)
        this.compareData(hareFile, bufferPool, dimensions)

        /** Cleanup. */
        hareFile.close()
    }

    /**
     * Initializes this [DirectHareDiskManager] with random data.
     *
     * @param size The number of [HarePage]s to write.
     */
    @ExperimentalTime
    private fun initWithData(hareFile: FixedHareColumnFile<DoubleVectorValue>, bufferPool: BufferPool, dimensions: Int) {
        var writeTime = Duration.ZERO
        val random = SplittableRandom(this.seed)
        val writer = FixedHareColumnWriter(hareFile, bufferPool)
        for (d in 0 until TestConstants.collectionSize) {
            writeTime += measureTime {
                writer.append(DoubleVectorValue.random(dimensions, random))
            }
        }
        val physSize = (bufferPool.diskSize `in` Units.MEGABYTE)

        /* Close writer. */
        writer.close()

        println("Writing ${TestConstants.collectionSize} doubles vectors (d=$dimensions) to a total of $physSize took $writeTime (${physSize.value / writeTime.inSeconds} MB/s).")
    }

    /**
     * Compares the data stored in this [DirectHareDiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareData(hareFile: FixedHareColumnFile<DoubleVectorValue>, bufferPool: BufferPool, dimensions: Int) {
        val cursor = FixedHareColumnCursor(hareFile, bufferPool)
        val reader = FixedHareColumnReader(hareFile, bufferPool)
        val random = SplittableRandom(this.seed)
        var read = 0L
        val readTime = measureTime {
            for (tupleId in cursor) {
                val doubleVectorValue = reader.get(tupleId)
                Assertions.assertArrayEquals(DoubleVectorValue.random(dimensions, random).data, doubleVectorValue?.data)
                read++
            }
            Assertions.assertEquals(reader.count(), read)
        }
        val physSize = (bufferPool.diskSize `in` Units.MEGABYTE)

        /* Close reader and cursor. */
        reader.close()
        cursor.close()

        println("Reading ${TestConstants.collectionSize} doubles vectors (d=$dimensions) to a total of $physSize took $readTime (${physSize.value / readTime.inSeconds} MB/s).")
    }
}