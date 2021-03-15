package org.vitrivr.cottontail.database.column.hare

import org.vitrivr.cottontail.config.HareConfig
import org.vitrivr.cottontail.database.column.*
import org.vitrivr.cottontail.database.column.mapdb.MapDBColumn
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.database.general.TxSnapshot
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnCursor
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnReader
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnWriter
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnWriter
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.buffer.eviction.EvictionPolicy
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.StampedLock

/**
 * Represents a single column in the Cottontail DB model. A [HareColumn] record is identified by a tuple ID (long)
 * and can hold an arbitrary value. Usually, multiple [MapDBColumn]s make up an [Entity].
 *
 * @see Entity
 *
 * @param <T> Type of the value held by this [MapDBColumn].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class HareColumn<T : Value>(override val name: Name.ColumnName, override val parent: Entity) : Column<T> {

    companion object {
        /**
         * Initializes a new, empty [MapDBColumn]
         *
         * @param definition The [ColumnDef] that specified the [HareColumnFile]
         * @param location The [Path] in which the [HareColumnFile] will be stored.
         * @param config The [HareConfig] used to initialize the [HareColumnFile]
         *
         * @return The [Path] of the [HareColumn]
         */
        fun initialize(definition: ColumnDef<*>, location: Path, config: HareConfig): Path {
            val path = location.resolve("${definition.name.simple}.${HareColumnFile.SUFFIX}")
            FixedHareColumnFile.createDirect(path, definition)
            return path
        }
    }

    /** The [Path] to the [HareColumn]'s data file. */
    override val path: Path = parent.path.resolve("${name.simple}.hare")

    /** The [FixedHareColumnFile] that backs this [HareColumn]. */
    private val column = FixedHareColumnFile<T>(this.path)

    /** This [HareColumn]'s [ColumnDef]. */
    override val columnDef: ColumnDef<T> =
        ColumnDef(this.name, this.column.type, this.column.nullable)

    /** An internal lock that is used to synchronize structural changes to an [MapDBColumn] (e.g. closing or deleting) with running [MapDBColumn.Tx]. */
    private val closeLock = StampedLock()

    /** Flag indicating whether this [HareColumn] is open or closed. */
    override var closed: Boolean = false
        private set

    override fun newTx(context: TransactionContext): ColumnTx<T> = Tx(context)

    override val maxTupleId: TupleId
        get() = TODO()

    override val engine: ColumnEngine
        get() = ColumnEngine.HARE

    override val version: DBOVersion
        get() = DBOVersion.V2_0

    /**
     * Closes the [HareColumn]. Closing an [HareColumn] is a delicate matter since ongoing [HareColumn.Tx] might be involved.
     * Therefore, access to the method is mediated by an global [HareColumn] wide lock.
     */
    override fun close() = this.closeLock.write {
        this.column.close()
        this.closed = true
    }

    /**
     * A [ColumnTx] that affects this [HareColumn].
     */
    inner class Tx constructor(context: TransactionContext) : AbstractTx(context), ColumnTx<T> {

        override val snapshot: TxSnapshot
            get() = TODO("Not yet implemented")

        /** Reference to the [HareColumn] this [HareColumn.Tx] belongs to. */
        override val dbo: Column<T>
            get() = this@HareColumn

        /** The [ColumnDef] of the [Column] underlying this [ColumnTransaction]. */
        override val columnDef: ColumnDef<T>
            get() = this@HareColumn.columnDef

        /** Obtains a global (non-exclusive) read-lock on [HareColumn]. Prevents enclosing [HareColumn] from being closed while this [HareColumn.Tx] is still in use. */
        private val globalStamp = this@HareColumn.closeLock.readLock()

        /** Shared [BufferPool] for this [Tx]. */
        private val bufferPool = BufferPool(this@HareColumn.column.disk, context.txId, 5, EvictionPolicy.LRU)

        /** [FixedHareColumnReader] for this [Tx]. */
        private val reader = FixedHareColumnReader(this@HareColumn.column, this.bufferPool)

        /** [FixedHareColumnReader] for this [Tx]. */
        private val writer: HareColumnWriter<T> = FixedHareColumnWriter(this@HareColumn.column, this.bufferPool)

        override fun count(): Long = this.withReadLock {
            this.reader.count()
        }

        override fun read(tupleId: Long): T? = this.withReadLock {
            this.reader.get(tupleId)
        }

        override fun insert(record: T?) = this.withWriteLock {
            this.writer.append(record)
        }

        override fun update(tupleId: TupleId, value: T?) = this.withWriteLock {
            val old = this.reader.get(tupleId)
            this.writer.update(tupleId, value)
            old
        }

        override fun compareAndUpdate(tupleId: Long, value: T?, expected: T?): Boolean = this.withWriteLock {
            this.writer.compareAndUpdate(tupleId, expected, value)
        }

        override fun delete(tupleId: TupleId) = withWriteLock {
            this.writer.delete(tupleId)
        }

        override fun scan(): ColumnCursor<T> = this.scan(0L..this@HareColumn.maxTupleId)

        override fun scan(range: LongRange) = object : ColumnCursor<T> {

            init {
                this@Tx.withReadLock { /* No op. */ }
            }

            /** [FixedHareColumnCursor] instance used for this [ColumnCursor]. */
            private val cursor = FixedHareColumnCursor(this@HareColumn.column, this@Tx.bufferPool, range)

            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean = this.cursor.hasNext()

            /**
             * Returns the next [TupleId] in the iteration.
             */
            override fun next(): TupleId = this.cursor.next()

            /**
             * Reads the value at the current [ColumnCursor] position and returns it.
             *
             * @return The value [T] at the position of this [ColumnCursor].
             */
            override fun readThrough(): T? = this@Tx.reader.get(this.next())
        }


        /**
         * Releases the [closeLock] on the [MapDBColumn].
         */
        override fun cleanup() {
            this@HareColumn.closeLock.unlockRead(this.globalStamp)
        }
    }
}