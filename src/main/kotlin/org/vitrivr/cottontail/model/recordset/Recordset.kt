package org.vitrivr.cottontail.model.recordset

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.model.basics.Filterable
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.Scanable
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.util.*
import java.util.concurrent.locks.StampedLock
import kotlin.math.min

/**
 * A [Recordset] as returned and processed by Cottontail DB. [Recordset]s are tables. A [Recordset]'s
 * columns are defined by the [ColumnDef]'s it contains ([Recordset.columns] and may contain an arbitrary
 * number of [Record] entries as rows.
 *
 * [Recordset]s are the unit of in-memory data storage and processing in Cottontail DB.
 *
 * @see org.vitrivr.cottontail.database.entity.DefaultEntity
 *
 * @author Ralph Gasser
 * @version 1.7.0
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
    fun addRow(record: Record) = this.addRow(record.tupleId, record.columns.map { record[it] }.toTypedArray())

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
     * Checks if this [Filterable] can process the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean =
        predicate is BooleanPredicate &&
                predicate.columns.all { this.columns.contains(it) }

    /**
     *
     */
    override fun filter(predicate: Predicate): Iterator<Record> {
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
     * Returns an [Iterator] for this [Recordset]. The [Iterator] is NOT thread safe.
     *
     * @return [Iterator] of this [Recordset].
     */
    fun iterator() = this.scan(this.columns)

    /**
     * Returns an [Iterator] for this [Recordset] for the given [columns]. The [Iterator] is NOT thread safe.
     *
     * @param columns The [ColumnDef] to include in the iteration.
     * @return [Iterator] of this [Recordset].
     */
    override fun scan(columns: Array<ColumnDef<*>>): Iterator<Record> = this.scan(columns, 0, 1)

    /**
     * Returns an [Iterator] for this [Recordset] for the given [columns] and the given [range].
     * The [Iterator] is NOT thread safe.
     *
     * @param columns The [ColumnDef] to include in the iteration.
     * @param partitionIndex The [partitionIndex] for this [scan] call.
     * @param partitions The total number of partitions for this [scan] call.
     * @return [Iterator] of this [Recordset].
     */
    override fun scan(columns: Array<ColumnDef<*>>, partitionIndex: Int, partitions: Int) = object : Iterator<Record> {

        /** The [LongRange] to iterate over. */
        private val range: LongRange

        init {
            val maximum: Long = this@Recordset.rowCount
            val partitionSize: Long = Math.floorDiv(maximum, partitions.toLong()) + 1L
            val start: Long = partitionIndex * partitionSize
            val end = min(((partitionIndex + 1) * partitionSize), maximum)
            this.range = start until end
        }

        /** Internal pointer kept as reference to the next [Record]. */
        @Volatile
        private var pointer = range.first

        /**
         * Returns true if the next invocation of [Iterator#next()] returns a value and false otherwise.
         *
         * @return Boolean indicating, whether this [Iterator] will return a value.
         */
        override fun hasNext(): Boolean {
            return this.pointer < this@Recordset.list.size64()
        }

        /**
         * Returns the next value of this [Iterator].
         *
         * @return Next [Record] of this [Iterator].
         */
        override fun next(): Record {
            val record = this@Recordset.list[this.pointer]
            this.pointer += 1
            return record
        }
    }

    /**
     * A [Record] implementation that depends on the existence of the enclosing [Recordset].
     *
     * @author Ralph Gasser
     * @version 1.0.1
     */
    inner class RecordsetRecord(override var tupleId: Long, values: Array<Value?> = Array(this@Recordset.columns.size) { null }) : Record {
        init {
            /** Sanity check. */
            require(values.size == this.columns.size) { "The number of values must be equal to the number of columns held by the StandaloneRecord (v = ${values.size}, c = ${this.columns.size})" }
            this.columns.forEachIndexed { index, columnDef ->
                if (!columnDef.validate(values[index])) {
                    throw IllegalArgumentException("Provided value ${values[index]} is incompatible with column ${columnDef}.")
                }
            }
        }

        /** Array of [ColumnDef]s that describes the [Column][org.vitrivr.cottontail.database.column.Column] of this [Record]. */
        override val columns: Array<ColumnDef<*>>
            get() = this@Recordset.columns

        /** Initialize internal [Object2ObjectOpenHashMap] used to map columns to values. */
        private val map = Object2ObjectOpenHashMap(this.columns, values)

        /**
         * Copies this [RecordsetRecord] and returns the copy as [StandaloneRecord].
         *
         * @return Copy of this [RecordsetRecord] as [StandaloneRecord].
         */
        override fun copy(): Record = StandaloneRecord(this.tupleId, this.columns, this.columns.map { this.map[it] }.toTypedArray())

        /**
         * Iterates over the [ColumnDef] and [Value] pairs in this [Record] in the order specified by [columns].
         *
         * @param action The action to apply to each [ColumnDef], [Value] pair.
         */
        override fun forEach(action: (ColumnDef<*>, Value?) -> Unit) {
            for (c in this.columns) action(c, this.map[c])
        }

        /**
         * Returns true, if this [StandaloneRecord] contains the specified [ColumnDef] and false otherwise.
         *
         * @param column The [ColumnDef] specifying the column
         * @return True if record contains the [ColumnDef], false otherwise.
         */
        override fun has(column: ColumnDef<*>): Boolean = this.map.containsKey(column)

        /**
         * Returns column index of the given [ColumnDef] within this [Record]. Returns -1 if [ColumnDef] is not contained
         *
         * @param column The [ColumnDef] to check.
         * @return The column index or -1. of [ColumnDef] is not part of this [Record].
         */
        override fun indexOf(column: ColumnDef<*>): Int = this@Recordset.columns.indexOf(column)

        /**
         * Returns an unmodifiable [Map] of the data contained in this [StandaloneRecord].
         *
         * @return Unmodifiable [Map] of the data in this [StandaloneRecord].
         */
        override fun toMap(): Map<ColumnDef<*>, Value?> = Collections.unmodifiableMap(this.map)

        /**
         * Retrieves the value for the specified [ColumnDef] from this [StandaloneRecord].
         *
         * @param column The [ColumnDef] for which to retrieve the value.
         * @return The value for the [ColumnDef]
         */
        override fun get(column: ColumnDef<*>): Value? {
            require(this.map.contains(column)) { "The specified column ${column.name}  (type=${column.type.name}) is not contained in this record." }
            return this.map[column]
        }

        /**
         * Sets the value for the specified [ColumnDef] in this [StandaloneRecord].
         *
         * @param column The [ColumnDef] for which to set the value.
         * @param value The new value for the [ColumnDef]
         */
        override fun set(column: ColumnDef<*>, value: Value?) {
            require(this.map.contains(column)) { "The specified column ${column.name}  (type=${column.type.name}) is not contained in this record." }
            if (!column.validate(value)) {
                throw IllegalArgumentException("Provided value $value is incompatible with column $column.")
            }
            this.map[column] = value
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as RecordsetRecord

            if (tupleId != other.tupleId) return false
            if (map != other.map) return false

            return true
        }

        override fun hashCode(): Int {
            var result = tupleId.hashCode()
            result = 31 * result + columns.hashCode()
            result = 31 * result + map.hashCode()
            return result
        }
    }


}