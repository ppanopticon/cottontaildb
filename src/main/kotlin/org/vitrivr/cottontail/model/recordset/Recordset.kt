package org.vitrivr.cottontail.model.recordset

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList
import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.database.queries.components.Predicate
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.util.concurrent.locks.StampedLock

/**
 * A [Recordset] as returned and processed by Cottontail DB. [Recordset]s are tables. A [Recordset]'s columns are defined by the [ColumnDef]'s
 * it contains ([Recordset.columns] and it contains an arbitrary number of [Record] entries as rows.
 *
 * [Recordset]s are the unit of data retrieval and processing in Cottontail DB. Whenever information is accessed through an [Entity][org.vitrivr.cottontail.database.entity.Entity],
 * a [Recordset] is being generated. Furthermore, the entire query execution pipeline processes, transforms and produces [Recordset]s.
 *
 * @see org.vitrivr.cottontail.database.entity.Entity
 *
 * @author Ralph Gasser
 * @version 1.5.1
 */
class Recordset(val columns: Array<ColumnDef<*>>, capacity: Long = 250L) : Scanable, Filterable {
    /** List of all the [Record]s contained in this [Recordset] (TupleId -> Record). */
    private val list = ObjectBigArrayBigList<Record>(capacity)

    /** [StampedLock] that mediates access to this [Recordset]. */
    private val lock = StampedLock()

    /** The number of columns contained in this [Recordset]. */
    val columnCount: Int
        get() = this.columns.size

    /** The number of rows contained in this [Recordset]. */
    val rowCount: Long
        get() {
            var stamp = this.lock.tryOptimisticRead()
            val size = this.list.size64()
            return if (this.lock.validate(stamp)) {
                size
            } else {
                try {
                    stamp = this.lock.readLock()
                    size
                } finally {
                    this.lock.unlockRead(stamp)
                }
            }
        }

    /**
     * Creates a new [Record] given the provided tupleId and values and appends it to this [Recordset].
     *
     * @param tupleId The [TupleId] of the new [Record].
     * @param values The values to add to this [Recordset].
     */
    fun addRow(tupleId: TupleId, values: Array<Value?>) = this.lock.write {
        this.list.add(RecordsetRecord(tupleId, values))
    }

    /**
     * Creates a new [Record] and appends it to this [Recordset]. The [TupleId] of the new [Record]
     * is deduced from the number of [Record]s in the [Recordset]. I.e., it is not safe to assume
     * that this method produces unique [TupleId], unless all [Record]s have only been added through
     * this method.
     *
     * @param values The values to add to this [Recordset].
     */
    fun addRow(values: Array<Value?>) = this.addRow(this.list.size64(), values)

    /**
     * Appends a [Record] (without a tupleId) to this [Recordset].
     *
     * @param record The record to add to this [Recordset].
     */
    fun addRow(record: Record) = this.addRow(record.tupleId, record.values)

    /**
     * Retrieves the value for the specified tuple ID from this [Recordset].
     *
     * @param tupleId The [TupleId] for which to return the [Record]
     * @return The [Record]
     */
    operator fun get(tupleId: TupleId): Record {
        var stamp = this.lock.tryOptimisticRead()
        val value = this.list[tupleId]
        return if (this.lock.validate(stamp)) {
            value
        } else {
            try {
                stamp = this.lock.readLock()
                this.list[tupleId]
            } finally {
                this.lock.unlockRead(stamp)
            }
        }
    }

    /**
     * Creates and returns a new [Recordset] by building the union between this and the provided [Recordset]
     *
     * @param other The [Recordset] to union this [Recordset] with.
     * @return combined [Recordset]
     */
    fun union(other: Recordset): Recordset = this.lock.read {
        if (!other.columns.contentDeepEquals(this.columns)) {
            throw IllegalArgumentException("UNION of record sets not possible; columns of the two record sets are not the same!")
        }
        return Recordset(this.columns).also { new ->
            (0L until this.list.size64()).forEach {
                new.addRow(this.list[it])
            }
            (0L until this.list.size64()).forEach {
                new.addRow(this.list[it])
            }
        }
    }

    /**
     * Creates and returns a new [Recordset] by building the intersection between this and the provided [Recordset]
     *
     * @param other The [Recordset] to intersect this [Recordset] with.
     * @return combined [Recordset]
     */
    fun intersect(other: Recordset): Recordset = this.lock.read {
        if (!other.columns.contentDeepEquals(this.columns)) {
            throw IllegalArgumentException("INTERSECT of record sets not possible; columns of the two record sets are not the same!")
        }
        return Recordset(this.columns).also { new ->
            val map = Long2LongOpenHashMap()
            (0L until this.list.size64()).forEach {
                map[this.list[it].tupleId] = it
            }

            (0L until other.list.size64()).forEach {
                val record = this.list[it]
                if (map.contains(record.tupleId)) {
                    new.addRow(record)
                }
            }
        }
    }

    /**
     * Returns the first [Record] in this [Recordset].
     *
     * @return The first [Record] of this [Recordset]
     */
    fun first(): Record? = this.list.first()

    /**
     * Applies the provided action to each [Record] in this [Recordset].
     *
     * @param action The action that should be applied.
     */
    fun forEachIndexed(action: (Int, Record) -> Unit) = this.lock.read {
        this.list.forEachIndexed(action)
    }

    /**
     * Checks if this [Filterable] can process the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean = predicate is BooleanPredicate && predicate.columns.all { this.columns.contains(it) }

    /**
     *
     */
    override fun filter(predicate: Predicate): CloseableIterator<Record> {
        TODO("Not yet implemented")
    }

    /**
     * Returns the [ColumnDef] for the specified column index.
     *
     * @param column The index of the desired [ColumnDef]
     * @return [ColumnDef] for the specified index.
     */
    fun column(column: Int): ColumnDef<*> = this.columns[column]

    /**
     * Returns the index for the specified [ColumnDef] or -1, if this [Recordset] does not contain that [ColumnDef].
     *
     * @param column The desired [ColumnDef]
     * @return Index of the specified [ColumnDef].
     */
    fun indexOf(column: ColumnDef<*>): Int = this.columns.indexOf(column)

    /**
     * Returns a list view of this [Recordset]. If this [Recordset] contains more than [Int.MAX_VALUE] [Record]s,
     * these [Record]s will be lost!
     *
     * @return The [List] underpinning this [Recordset].
     */
    fun toList(): List<Record> = this.list.toList()

    /**
     * Returns an [Iterator] for this [Recordset]. As long as this [Iterator] exists it will retain a read lock on this [Recordset].
     * <strong>Important:</strong> The implementation of this [CloseableIterator] is NOT thread safe.
     *
     * @return [Iterator] of this [Recordset].
     */
    fun iterator(): CloseableIterator<Record> = object: CloseableIterator<Record> {

        /** Obtains a stamped read lock from the surrounding [Recordset]. */
        private val stamp = this@Recordset.lock.readLock()

        /** Flag indicating whether this [CloseableIterator] has been closed.*/
        @Volatile
        private var closed = false

        /** Internal pointer kept as reference to the next [Record]. */
        @Volatile
        private var pointer = 0L

        /**
         * Returns true if the next invocation of [CloseableIterator#next()] returns a value and false otherwise.
         *
         * @return Boolean indicating, whether this [CloseableIterator] will return a value.
         */
        override fun hasNext(): Boolean {
            if (this.closed) throw IllegalStateException("Illegal invocation of hasNext(): This CloseableIterator has been closed.")
            return this.pointer < this@Recordset.list.size64()
        }

        /**
         * Returns the next value of this [CloseableIterator].
         *
         * @return Next [Record] of this [CloseableIterator].
         */
        override fun next(): Record {
            if (this.closed) throw IllegalStateException("Illegal invocation of next(): This CloseableIterator has been closed.")
            val record = this@Recordset.list[this.pointer]
            this.pointer += 1
            return record
        }

        /**
         * Closes this [CloseableIterator].
         */
        override fun close() {
            if (!this.closed) {
                this.closed = true
                this@Recordset.lock.unlockRead(this.stamp)
            }
        }

        /**
         * Closes this [CloseableIterator] upon finalization.
         */
        protected fun finalize() {
            this.close()
        }
    }

    /**
     * A [Record] implementation that depends on the existence of the enclosing [Recordset].
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class RecordsetRecord(override val tupleId: Long, override val values: Array<Value?>) : Record {

        /** Array of [ColumnDef]s that describes the [Column][org.vitrivr.cottontail.database.column.Column] of this [Record]. */
        override val columns: Array<ColumnDef<*>>
            get() = this@Recordset.columns

        init {
            /** Sanity check. */
            require(this.values.size == this.columns.size) { "The number of values must be equal to the number of columns held by the StandaloneRecord (v = ${this.values.size}, c = ${this.columns.size})" }
            this.columns.forEachIndexed { index, columnDef ->
                columnDef.validateOrThrow(this.values[index])
            }
        }

        /**
         * Copies this [Record] and returns the copy.
         *
         * @return Copy of this [Record]
         */
        override fun copy(): Record = StandaloneRecord(tupleId, columns = this.columns, this.values.copyOf())

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Record

            if (tupleId != other.tupleId) return false
            if (!columns.contentEquals(other.columns)) return false
            if (!values.contentEquals(other.values)) return false

            return true
        }

        override fun hashCode(): Int {
            var result = tupleId.hashCode()
            result = 31 * result + columns.hashCode()
            result = 31 * result + values.contentHashCode()
            return result
        }
    }

    override fun scan(columns: Array<ColumnDef<*>>): CloseableIterator<Record> {
        TODO("Not yet implemented")
    }

    override fun scan(columns: Array<ColumnDef<*>>, range: LongRange): CloseableIterator<Record> {
        TODO("Not yet implemented")
    }


}