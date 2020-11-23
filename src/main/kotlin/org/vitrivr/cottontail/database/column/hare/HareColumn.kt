package org.vitrivr.cottontail.database.column.hare

import org.vitrivr.cottontail.config.HareConfig
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.column.ColumnCursor
import org.vitrivr.cottontail.database.column.ColumnTransaction
import org.vitrivr.cottontail.database.column.mapdb.MapDBColumn
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.Transaction
import org.vitrivr.cottontail.database.general.TransactionStatus
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.exceptions.TransactionException
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnCursor
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnReader
import org.vitrivr.cottontail.storage.engine.hare.access.column.fixed.FixedHareColumnWriter
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnFile
import org.vitrivr.cottontail.storage.engine.hare.access.interfaces.HareColumnWriter
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.buffer.eviction.EvictionPolicy
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
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
class HareColumn<T : Value>(override val name: Name.ColumnName, override val catalogue: Catalogue) : Column<T> {

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

    /** Provides access to the [Entity] this [HareColumn] belongs to. */
    override val parent: Entity
        get() = this.catalogue.instantiateEntity(this.name.entity()!!)

    /** The [Path] to the file backing this [MapDBColumn]. */
    override val path: Path = this.catalogue.columnForName(this.name).path

    /** The [FixedHareColumnFile] that backs this [HareColumn]. */
    private val column = FixedHareColumnFile<T>(this.path)

    /** This [HareColumn]'s [ColumnDef]. */
    override val columnDef: ColumnDef<T> = ColumnDef(this.name, this.column.columnType, this.column.logicalSize, this.column.nullable)

    /** An internal lock that is used to synchronize concurrent read & write access to this [MapDBColumn] by different [MapDBColumn.Tx]. */
    private val txLock = StampedLock()

    /** An internal lock that is used to synchronize structural changes to an [MapDBColumn] (e.g. closing or deleting) with running [MapDBColumn.Tx]. */
    private val globalLock = StampedLock()

    /** Flag indicating whether this [HareColumn] is open or closed. */
    override var closed: Boolean = false
        private set

    override fun newTransaction(readonly: Boolean, tid: UUID): ColumnTransaction<T> = Tx(readonly, tid)

    /**
     * Closes the [HareColumn]. Closing an [HareColumn] is a delicate matter since ongoing [HareColumn.Tx] might be involved.
     * Therefore, access to the method is mediated by an global [HareColumn] wide lock.
     */
    override fun close() = this.globalLock.write {
        this.column.close()
        this.closed = true
    }

    /**
     * A [Transaction] that affects this [HareColumn].
     */
    inner class Tx constructor(override val readonly: Boolean, override val tid: UUID) : ColumnTransaction<T> {
        /** Flag indicating the [TransactionStatus] of this [HareColumn.Tx]. */
        @Volatile
        override var status: TransactionStatus = TransactionStatus.CLEAN
            private set

        /** The [ColumnDef] of the [Column] underlying this [ColumnTransaction]. */
        override val columnDef: ColumnDef<T>
            get() = this@HareColumn.columnDef

        /** Obtains a global (non-exclusive) read-lock on [HareColumn]. Prevents enclosing [HareColumn] from being closed while this [HareColumn.Tx] is still in use. */
        private val globalStamp = this@HareColumn.globalLock.readLock()

        /** Obtains transaction lock on [HareColumn]. Prevents concurrent read & write access to the enclosing [MapDBColumn]. */
        private val txStamp = if (this.readonly) {
            this@HareColumn.txLock.readLock()
        } else {
            this@HareColumn.txLock.writeLock()
        }

        /** A [StampedLock] local to this [HareColumn.Tx]. It makes sure, that this [HareColumn.Tx] cannot be committed, closed or rolled back while it is being used. */
        private val localLock = StampedLock()

        /** Shared [BufferPool] for this [Tx]. */
        private val bufferPool = BufferPool(this@HareColumn.column.disk, this.tid, 5, EvictionPolicy.LRU)

        /** [FixedHareColumnReader] for this [Tx]. */
        private val reader = FixedHareColumnReader(this@HareColumn.column, this.bufferPool)

        /** [FixedHareColumnReader] for this [Tx]. */
        private val writer: HareColumnWriter<T> = if (!this.readonly) {
            FixedHareColumnWriter(this@HareColumn.column, this.bufferPool)
        } else {
            ReadonlyHareColumnWriter(this.tid)
        }

        override fun count(): Long = this.localLock.read {
            checkValidForRead()
            this.reader.count()
        }

        override fun maxTupleId(): TupleId = this.localLock.read {
            checkValidForRead()
            this.reader.maxTupleId()
        }

        override fun read(tupleId: Long): T? = this.localLock.read {
            checkValidForRead()
            this.reader.get(tupleId)
        }

        override fun insert(record: T?) = this.localLock.read {
            checkValidForWrite()
            this.writer.append(record)
        }

        override fun update(tupleId: TupleId, value: T?) = this.localLock.read {
            checkValidForWrite()
            this.writer.update(tupleId, value)
        }

        override fun compareAndUpdate(tupleId: Long, value: T?, expected: T?): Boolean = this.localLock.read {
            checkValidForWrite()
            this.writer.compareAndUpdate(tupleId, expected, value)
        }

        override fun delete(tupleId: TupleId) = this.localLock.read {
            checkValidForWrite()
            this.writer.delete(tupleId)
            Unit
        }

        override fun scan(): ColumnCursor<T> = this.scan(0L..this.maxTupleId())

        override fun scan(range: LongRange) = object : ColumnCursor<T> {
            init {
                checkValidForRead()
            }

            /** Acquires a read lock on the surrounding [MapDBColumn.Tx]*/
            private val lock = this@Tx.localLock.readLock()

            /** [FixedHareColumnCursor] instance used for this [ColumnCursor]. */
            private val cursor = FixedHareColumnCursor(this@HareColumn.column, this@Tx.bufferPool, range)

            /** Flag indicating whether this [ColumnCursor] has been closed. */
            override val isOpen: Boolean
                get() = cursor.isOpen

            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean {
                check(this.isOpen) { "Illegal invocation of next(): This CloseableIterator has been closed." }
                return this.cursor.hasNext()
            }

            /**
             * Returns the next [TupleId] in the iteration.
             */
            override fun next(): TupleId {
                check(this.isOpen) { "Illegal invocation of next(): This CloseableIterator has been closed." }
                return this.cursor.next()
            }

            /**
             * Reads the value at the current [ColumnCursor] position and returns it.
             *
             * @return The value [T] at the position of this [ColumnCursor].
             */
            override fun readThrough(): T? = this@Tx.reader.get(this.next())

            /**
             * Closes this [ColumnCursor] and releases all locks associated with it.
             */
            override fun close() {
                if (this.isOpen) {
                    this.cursor.close()
                    this@Tx.localLock.unlockRead(this.lock)
                }
            }
        }

        /**
         * Commits all changes made through this [Tx] since the last commit or rollback.
         */
        override fun commit() = this.localLock.write {
            if (this.status == TransactionStatus.DIRTY) {
                this.writer.commit()
                this.status = TransactionStatus.CLEAN
            }
        }

        /**
         * Rolls all changes made through this [Tx] back to the last commit. Can only be executed, if [Tx] is
         * in status [TransactionStatus.DIRTY] or [TransactionStatus.ERROR].
         */
        override fun rollback() = this.localLock.write {
            if (this.status == TransactionStatus.DIRTY || this.status == TransactionStatus.ERROR) {
                this.writer.rollback()
                this.status = TransactionStatus.CLEAN
            }
        }

        /**
         * Closes this [Tx] and relinquishes the associated [ReentrantReadWriteLock].
         */
        override fun close() = this.localLock.write {
            if (this.status != TransactionStatus.CLOSED) {
                if (this.status == TransactionStatus.DIRTY || this.status == TransactionStatus.ERROR) {
                    this.rollback()
                }
                this.status = TransactionStatus.CLOSED

                /* Close reader & writer. */
                this.reader.close()
                this.writer.close()

                /* Release locks. */
                this@HareColumn.txLock.unlock(this.txStamp)
                this@HareColumn.globalLock.unlockRead(this.globalStamp)
            }
        }

        /**
         * Checks if this [HareColumn.Tx] is still open. Otherwise, an exception will be thrown.
         */
        private fun checkValidForRead() {
            if (this.status == TransactionStatus.CLOSED) throw TransactionException.TransactionClosedException(this.tid)
            if (this.status == TransactionStatus.ERROR) throw TransactionException.TransactionInErrorException(this.tid)
        }

        /**
         * Tries to acquire a write-lock. If method fails, an exception will be thrown
         */
        private fun checkValidForWrite() {
            if (this.readonly) throw TransactionException.TransactionReadOnlyException(this.tid)
            if (this.status == TransactionStatus.CLOSED) throw TransactionException.TransactionClosedException(this.tid)
            if (this.status == TransactionStatus.ERROR) throw TransactionException.TransactionInErrorException(this.tid)
            if (this.status != TransactionStatus.DIRTY) {
                this.status = TransactionStatus.DIRTY
            }
        }
    }
}