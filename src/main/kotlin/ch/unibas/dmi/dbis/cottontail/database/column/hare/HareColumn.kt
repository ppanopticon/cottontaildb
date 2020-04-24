package ch.unibas.dmi.dbis.cottontail.database.column.hare

import ch.unibas.dmi.dbis.cottontail.database.column.Column
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnTransaction
import ch.unibas.dmi.dbis.cottontail.database.column.mapdb.MapDBColumn
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.Transaction
import ch.unibas.dmi.dbis.cottontail.database.general.TransactionStatus
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Filterable
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.exceptions.TransactionException
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.column.FixedHareColumnFile
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.exclusive
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.StampedLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Represents a single column in the Cottontail DB model. A [MapDBColumn] record is identified by a tuple ID (long)
 * and can hold an arbitrary value. Usually, multiple [MapDBColumn]s make up an [Entity].
 *
 * @see Entity
 *
 * @param <T> Type of the value held by this [MapDBColumn].
 *
 * @author Ralph Gasser
 * @version 1.2
 */
class HareColumn<T : Value>(override val name: Name, override val parent: Entity) : Column<T> {

    /** Constant FQN of the [Schema] object. */
    override val fqn: Name = this.parent.fqn.append(this.name)

    /** The [Path] to the [Entity]'s main folder. */
    override val path: Path = parent.path.resolve("col_$name.db")

    /** The [FixedHareColumnFile] file reference backing this [HareColumn]. */
    private val columnFile = FixedHareColumnFile<T>(this.path, false)

    /** The [FixedHareColumnFile] file reference backing this [HareColumn]. */
    private val header = this.columnFile.Header()

    override val columnDef: ColumnDef<T>
        get() = this.columnFile.columnDef

    override val maxTupleId: Long
        get() = this.header.count

    override var closed: Boolean = false
        private set

    /** An internal lock that is used to synchronize structural changes to an [HareColumn] (e.g. closing or deleting) with running [HareColumn.Tx]. */
    private val globalLock = StampedLock()

    /**
     *
     */
    override fun close() = this.globalLock.exclusive {
        if (!this.closed) {
            this.columnFile.close()
            this.closed = true
        }
    }

    /** An internal lock that is used to synchronize structural changes to an [HareColumn] (e.g. closing or deleting) with running [HareColumn.Tx]. */
    override fun newTransaction(readonly: Boolean, tid: UUID): ColumnTransaction<T> = Tx(readonly, tid)

    /**
     * Thinly veiled implementation of the [Record] interface for internal use.
     */
    inner class ColumnRecord(override val tupleId: Long, val value: Value?) : Record {
        override val columns
            get() = arrayOf(this@HareColumn.columnDef)
        override val values
            get() = arrayOf(this.value)

        override fun first(): Value? = this.value
        override fun last(): Value? = this.value
        override fun copy(): Record = ColumnRecord(this.tupleId, this.value)
    }

    /**
     * A [Transaction] that affects this [MapDBColumn].
     */
    inner class Tx constructor(override val readonly: Boolean, override val tid: UUID) : ColumnTransaction<T> {

        private val cursor = this@HareColumn.columnFile.cursor(!readonly)

        /** Obtains a global (non-exclusive) read-lock on [MapDBColumn]. Prevents enclosing [MapDBColumn] from being closed while this [MapDBColumn.Tx] is still in use. */
        private val globalStamp = this@HareColumn.globalLock.readLock()

        /** A [ReentrantReadWriteLock] local to this [Entity.Tx]. It makes sure, that this [Entity] cannot be committed, closed or rolled back while it is being used. */
        private val closeLock = ReentrantReadWriteLock()

        /** Flag indicating the status of this [Tx]. */
        @Volatile
        override var status: TransactionStatus = TransactionStatus.CLEAN
            private set


        override val columnDef: ColumnDef<T>
            get() = this@HareColumn.columnDef

        /**
         * Commits all changes made through this [Tx] since the last commit or rollback.
         */
        override fun commit() = this.closeLock.write {
            if (this.status == TransactionStatus.DIRTY) {
                this.status = TransactionStatus.CLEAN

                /* TODO: Commit. */
            }
        }

        /**
         * Rolls all changes made through this [Tx] back to the last commit. Can only be executed, if [Tx] is
         * in status [TransactionStatus.DIRTY] or [TransactionStatus.ERROR].
         */
        override fun rollback() = this.closeLock.write {
            if (this.status == TransactionStatus.DIRTY || this.status == TransactionStatus.ERROR) {
                this.status = TransactionStatus.CLEAN
                /* TODO: Commit. */
            }
        }

        /**
         * Closes this [Tx] and relinquishes the associated [ReentrantReadWriteLock].
         */
        override fun close() = this.closeLock.write {
            if (this.status != TransactionStatus.CLOSED) {
                if (this.status == TransactionStatus.DIRTY || this.status == TransactionStatus.ERROR) {
                    this.rollback()
                }
                this.cursor.close()
                this.status = TransactionStatus.CLOSED
            }
        }

        override fun count(): Long = this.closeLock.read {
            checkValidForRead()

            this@HareColumn.header.count
        }

        override fun read(tupleId: Long): T? = this.closeLock.read {
            checkValidForRead()

            this.cursor.get(tupleId)
        }

        override fun readAll(tupleIds: Collection<Long>) = this.closeLock.read {
            checkValidForRead()

            tupleIds.map { this.cursor.get(it) }
        }

        override fun insert(record: T?): Long = this.closeLock.read {
            checkValidForWrite()

            this.cursor.append(record)
        }

        override fun insertAll(records: Collection<T?>): Collection<Long> = this.closeLock.read {
            checkValidForWrite()

            records.map { this.cursor.append(it) }
        }

        override fun update(tupleId: Long, value: T?) = this.closeLock.read {
            checkValidForWrite()

            this.cursor.update(tupleId, value)
        }

        override fun compareAndUpdate(tupleId: Long, value: T?, expected: T?): Boolean = this.closeLock.read {
            checkValidForWrite()

            this.cursor.compareAndUpdate(tupleId, expected, value)
        }

        override fun delete(tupleId: Long) = this.closeLock.read {
            checkValidForWrite()

            this.cursor.delete(tupleId)
            Unit
        }

        override fun deleteAll(tupleIds: Collection<Long>) = this.closeLock.read {
            checkValidForWrite()

            tupleIds.forEach { this.cursor.delete(it) }
            Unit
        }

        override fun forEach(action: (Record) -> Unit) = this.closeLock.read {
            checkValidForRead()

            this.cursor.forEach { l, t ->
                action(ColumnRecord(l, t))
            }
        }

        override fun forEach(from: Long, to: Long, action: (Record) -> Unit) = this.closeLock.read {
            checkValidForRead()

            this.cursor.forEach(from, to) { l, t ->
                action(ColumnRecord(l, t))
            }
        }

        override fun canProcess(predicate: Predicate): Boolean = predicate is BooleanPredicate

        override fun filter(predicate: Predicate): Filterable = if (predicate is BooleanPredicate) {
            this.closeLock.read {
                checkValidForRead()
                val recordset = Recordset(arrayOf(this@HareColumn.columnDef))
                this.cursor.forEach { l, t ->
                    val data = ColumnRecord(l, t)
                    if (predicate.matches(data)) recordset.addRowUnsafe(data.values)
                }
                return recordset
            }
        } else {
            throw QueryException.UnsupportedPredicateException("HareColumn#forEach() does not support predicates of type '${predicate::class.simpleName}'.")
        }

        override fun forEach(from: Long, to: Long, predicate: Predicate, action: (Record) -> Unit) = if (predicate is BooleanPredicate) {
            this.closeLock.read {
                checkValidForRead()

                this.cursor.forEach(from, to) { l, t ->
                    val rec = ColumnRecord(l, t)
                    if (predicate.matches(rec)) {
                        action(rec)
                    }
                }
            }
        } else {
            throw QueryException.UnsupportedPredicateException("HareColumn#forEach() does not support predicates of type '${predicate::class.simpleName}'.")
        }

        override fun forEach(predicate: Predicate, action: (Record) -> Unit) = if (predicate is BooleanPredicate) {
            this.closeLock.read {
                checkValidForRead()

                this.cursor.forEach { l, t ->
                    val rec = ColumnRecord(l, t)
                    if (predicate.matches(rec)) {
                        action(rec)
                    }
                }
            }
        } else {
            throw QueryException.UnsupportedPredicateException("HareColumn#forEach() does not support predicates of type '${predicate::class.simpleName}'.")
        }

        override fun <R> map(action: (Record) -> R): Collection<R> {
            TODO("Not yet implemented")
        }

        override fun <R> map(from: Long, to: Long, action: (Record) -> R): Collection<R> {
            TODO("Not yet implemented")
        }

        override fun <R> map(predicate: Predicate, action: (Record) -> R): Collection<R> {
            TODO("Not yet implemented")
        }

        override fun <R> map(from: Long, to: Long, predicate: Predicate, action: (Record) -> R): Collection<R> {
            TODO("Not yet implemented")
        }

        /**
         *
         */
        private fun checkValidForRead() {
            if (this.status == TransactionStatus.CLOSED) throw TransactionException.TransactionClosedException(tid)
            if (this.status == TransactionStatus.ERROR) throw TransactionException.TransactionInErrorException(tid)
        }

        private fun checkValidForWrite() {
            if (this.readonly) throw TransactionException.TransactionReadOnlyException(tid)
            if (this.status == TransactionStatus.CLOSED) throw TransactionException.TransactionClosedException(tid)
            if (this.status == TransactionStatus.ERROR) throw TransactionException.TransactionInErrorException(tid)
            if (this.status != TransactionStatus.DIRTY) {
                this.status = TransactionStatus.DIRTY
            }
        }
    }
}