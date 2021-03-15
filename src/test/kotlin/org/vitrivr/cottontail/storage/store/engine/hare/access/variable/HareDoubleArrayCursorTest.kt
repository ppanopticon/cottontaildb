package org.vitrivr.cottontail.storage.store.engine.hare.access.variable

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.storage.basics.Units
import org.vitrivr.cottontail.storage.engine.hare.access.column.directory.Directory
import org.vitrivr.cottontail.storage.engine.hare.access.column.variable.VariableHareColumnCursor
import org.vitrivr.cottontail.storage.engine.hare.access.column.variable.VariableHareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.access.column.variable.VariableHareColumnReader
import org.vitrivr.cottontail.storage.engine.hare.access.column.variable.VariableHareColumnWriter
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

/**
 * Test case for [VariableHareColumnFile] data structure in combination with [DoubleVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class HareDoubleArrayCursorTest : AbstractCursorTest() {
    /** Path to column file. */
    val path = TestConstants.testDataPath.resolve("test-variable-double-vector-cursor-db.hare")

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
        val columnDef = ColumnDef(Name.ColumnName("test"), Type.DoubleVector(dimensions))
        VariableHareColumnFile.create(this.path, columnDef)
        val tid = 1L
        val hareFile: VariableHareColumnFile<DoubleVectorValue> = VariableHareColumnFile(this.path, false)
        val bufferPool = BufferPool(hareFile.disk, tid, 25, EvictionPolicy.LRU)

        this.initWithData(hareFile, bufferPool, dimensions)
        this.compareData(hareFile, bufferPool, dimensions)

        /** Cleanup. */
        hareFile.close()
    }

    /**
     * Compares the data stored in this [DirectHareDiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareData(hareFile: VariableHareColumnFile<DoubleVectorValue>, bufferPool: BufferPool, dimensions: Int) {
        val directory = Directory(hareFile, bufferPool)
        val cursor = VariableHareColumnCursor(hareFile, directory)
        val reader = VariableHareColumnReader(hareFile, directory)
        val random = SplittableRandom(this.seed)
        val readTime = measureTime {
            var read = 0L
            for (tupleId in cursor) {
                val doubleVectorValue = reader.get(tupleId)
                Assertions.assertArrayEquals(DoubleVectorValue.random(dimensions, random).data, doubleVectorValue?.data)
                read++
            }
            Assertions.assertEquals(reader.count(), read)
        }

        /* Close reader and cursor. */
        reader.close()

        val physSize = (bufferPool.diskSize `in` Units.MEGABYTE)
        println("Reading ${TestConstants.collectionSize} doubles vectors (d=$dimensions) to a total of $physSize took $readTime (${physSize.value / readTime.inSeconds} MB/s).")
    }

    /**
     * Initializes this [DirectHareDiskManager] with random data.
     *
     * @param size The number of [HarePage]s to write.
     */
    @ExperimentalTime
    private fun initWithData(hareFile: VariableHareColumnFile<DoubleVectorValue>, bufferPool: BufferPool, dimensions: Int) {
        var writeTime = Duration.ZERO
        val random = SplittableRandom(this.seed)
        val writer = VariableHareColumnWriter(hareFile, Directory(hareFile, bufferPool))
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
}