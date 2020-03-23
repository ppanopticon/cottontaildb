package ch.unibas.dmi.dbis.cottontail.storage.store.engine.hare.access

import ch.unibas.dmi.dbis.cottontail.database.column.IntVectorColumnType
import ch.unibas.dmi.dbis.cottontail.model.values.IntVectorValue
import java.nio.ByteBuffer

import ch.unibas.dmi.dbis.cottontail.database.column.DoubleColumnType
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.values.DoubleValue
import ch.unibas.dmi.dbis.cottontail.storage.basics.MemorySize
import ch.unibas.dmi.dbis.cottontail.storage.basics.Units
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column.HareColumn
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer.BufferPool
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DirectDiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import junit.framework.Assert.assertEquals
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

class ByteCursorTest {
    val path = Paths.get("./test-cursor-db.hare")

    var _manager: DirectDiskManager? = null

    var _pool: BufferPool? = null

    var columnDef = ColumnDef(Name("test"), IntVectorColumnType, 8092)

    var hare: HareColumn<IntVectorValue>? = null

    val random = SplittableRandom(System.currentTimeMillis())

    @BeforeEach
    fun beforeEach() {
        this._manager = DirectDiskManager(this.path)
        this._pool = BufferPool(this._manager!!)
        this.hare = HareColumn(this._pool!!, columnDef)
    }

    @AfterEach
    fun afterEach() {
        this._manager!!.close()
        Files.delete(this.path)
    }

    /**
     *
     */
    @Test
    @ExperimentalTime
    fun test512() {
        val data = ByteBuffer.wrap(ByteArray(8092))
        this.random.nextBytes(data.array())
        this.initWithData(1_000_00, data)
        this.compareData(data)
    }

    /**
     * Compares the data stored in this [DirectDiskManager] with the data provided as array of [ByteArray]s
     */
    @ExperimentalTime
    private fun compareData(data: ByteBuffer) {
        var readTime = Duration.ZERO
        val buffer = ByteBuffer.allocate(data.capacity())
        val cursor = this.hare!!.byteCursor(writeable = false)
        var read = 0L
        while (cursor.next()) {
            readTime += measureTime {
                read += cursor.read(buffer.rewind())
                assertEquals(0, data.rewind().compareTo(buffer.rewind()))
            }
        }
        println("Reading ${MemorySize(read.toDouble(), Units.BYTE) `in` Units.MEGABYTE} took $readTime (${(MemorySize(read.toDouble(), Units.BYTE) `in` Units.MEGABYTE).value / readTime.inSeconds} MB/s). ")
    }

    /**
     * Initializes this [DirectDiskManager] with random data.
     *
     * @param size The number of [Page]s to write.
     */
    @ExperimentalTime
    private fun initWithData(size: Int, data: ByteBuffer) {
        var writeTime = Duration.ZERO
        val cursor = this.hare!!.byteCursor(writeable = true)
        var written = 0L
        for (i in 0 until size) {
            writeTime += measureTime {
                cursor.append()
                written += cursor.write(data.rewind())
            }
        }
        cursor.close()
        println("Reading ${MemorySize(written.toDouble(), Units.BYTE) `in` Units.MEGABYTE} took $writeTime (${(MemorySize(written.toDouble(), Units.BYTE) `in` Units.MEGABYTE).value / writeTime.inSeconds} MB/s).")
    }
}