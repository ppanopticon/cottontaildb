package ch.unibas.dmi.dbis.cottontail.model.recordset

import ch.unibas.dmi.dbis.cottontail.database.queries.*
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Filterable
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.basics.Scanable
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.values.Value

import java.lang.IllegalArgumentException

import java.util.*
import java.util.concurrent.atomic.AtomicLong

/**
 * A record set as returned and processed by Cottontail DB. [Recordset]s are tables. Their columns are defined by the [ColumnDef]'s
 * it contains ([Recordset.columns] and it contains an arbitrary number of [Record] entries as rows.
 *
 * [Recordset]s are the unit of data retrieval and processing in Cottontail DB. Whenever information is accessed through an [Entity],
 * a [Recordset] is being generated. Furthermore, the entire query execution pipeline processes and produces [Recordset]s.
 *
 * @see Entity
 * @see QueryExecutionTask
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class Recordset(val columns: Array<ColumnDef<*>>) : Scanable, Filterable {

    /** Internal counter for maximum tupleId. */
    private val maxTupleId = AtomicLong(0)

    /** Map of all the [Record]s contained in this [Recordset] (TupleId -> Record). */
    private val map = TreeMap<Long,Record>()

    /** The number of columns contained in this [Recordset]. */
    val columnCount: Int
        get() = this.columns.size

    /** The number of rows contained in this [Recordset]. */
    val rowCount: Int
        get() = this.map.size


    /**
     * Creates a new [Record] and appends it to this [Recordset]. This is a potentially unsafe operation.
     *
     * @param tupleId The tupleId of the new [Record].
     * @param values The values to add to this [Recordset].
     */
    @Synchronized
    fun addRowUnsafe(values: Array<Value<*>?>) {
        val next = this.maxTupleId.incrementAndGet()
        this.map[next] = RecordsetRecord(next).assign(values)
    }

    /**
     * Creates a new [Record] given the provided tupleId and values and appends it to this [Recordset].
     * This is a potentially unsafe operation.
     *
     * @param tupleId The tupleId of the new [Record].
     * @param values The values to add to this [Recordset].
     */
    @Synchronized
    fun addRowUnsafe(tupleId: Long, values: Array<Value<*>?>) {
        this.map[this.maxTupleId.incrementAndGet()] = RecordsetRecord(tupleId).assign(values)
    }

    /**
     * Creates a new [Record] given the provided tupleId and values and appends them to this [Recordset]
     * if they match the provided [BooleanPredicate]. This is a potentially unsafe operation.
     *
     * @param tupleId The tupleId of the new [Record].
     * @param predicate The [BooleanPredicate] to match the [Record] against
     * @param values The values to add to this [Recordset].
     * @return True if [Record] was added, false otherwise.
     */
    @Synchronized
    fun addRowIfUnsafe(tupleId: Long, predicate: BooleanPredicate, values: Array<Value<*>?>): Boolean {
        val record = RecordsetRecord(tupleId).assign(values)
        return if (predicate.matches(record)) {
            this.map[this.maxTupleId.incrementAndGet()] = record
            true
        } else {
            false
        }
    }

    /**
     * Appends a [Record] (without a tupleId) to this [Recordset].
     *
     * @param values The record to add to this [Recordset].
     */
    @Synchronized
    fun addRow(record: Record) {
        if (record.columns.contentDeepEquals(this.columns)) {
            val tupleId = this.maxTupleId.incrementAndGet()
            this.map[tupleId] = RecordsetRecord(tupleId).assign(record.values)
        } else {
            throw IllegalArgumentException("The provided record (${this.columns.joinToString(".")}) is incompatible with this record set (${this.columns.joinToString(".")}.")
        }
    }

    /**
     * Creates a new [Record] given the provided tupleId and values and appends it to this [Recordset].
     *
     * @param tupleId The tupleId of the new [Record].
     * @param values The values to add to this [Recordset].
     */
    @Synchronized
    fun addRow(tupleId: Long, record: Record) {
        if (record.columns.contentDeepEquals(this.columns)) {
            this.map[this.maxTupleId.incrementAndGet()] = RecordsetRecord(tupleId).assign(record.values)
        } else {
            throw IllegalArgumentException("The provided record (${this.columns.joinToString(".")}) is incompatible with this record set (${this.columns.joinToString(".")}.")
        }
    }

    /**
     * Creates a new [Record] given the provided tupleId and values and appends them to this [Recordset]
     * if they match the provided [BooleanPredicate].
     *
     * @param tupleId The tupleId of the new [Record].
     * @param predicate The [BooleanPredicate] to match the [Record] against
     * @param values The values to add to this [Recordset].
     * @return True if [Record] was added, false otherwise.
     */
    @Synchronized
    fun addRowIf(tupleId: Long, predicate: BooleanPredicate, record: Record): Boolean {
        if (record.columns.contentDeepEquals(this.columns)) {
            return if (predicate.matches(record)) {
                this.map[this.maxTupleId.incrementAndGet()] = record
                true
            } else {
                false
            }
        } else {
            throw IllegalArgumentException("The provided record (${this.columns.joinToString(".")}) is incompatible with this record set (${this.columns.joinToString(".")}.")
        }
    }

    /**
     * Retrieves the value for the specified tuple ID from this [Recordset].
     *
     * @param tupleId The tuple ID for which to return the [Record]
     * @return The [Record]
     */
    @Synchronized
    operator fun get(tupleId: Long): Record? = this.map[tupleId]

    /**
     *
     */
    override fun listTupleIds(): Iterator<Long> = this.map.keys.iterator()

    /**
     * Creates and returns a new [Recordset] by building the union between this and the provided [Recordset]
     *
     * @param other The [Recordset] to union this [Recordset] with.
     * @return combined [Recordset]
     */
    fun union(other: Recordset): Recordset {
        if (!other.columns.contentDeepEquals(this.columns)) {
            throw IllegalArgumentException("UNION of record sets not possible; columns of the two record sets are not the same!")
        }

        return Recordset(this.columns).also {new ->
            other.forEach {
                new.addRow(it.tupleId, it)
            }
        }
    }

    /**
     * Creates and returns a new [Recordset] by building the intersection between this and the provided [Recordset]
     *
     * @param other The [Recordset] to intersect this [Recordset] with.
     * @return combined [Recordset]
     */
    fun intersect(other: Recordset): Recordset {
        if (!other.columns.contentDeepEquals(this.columns)) {
            throw IllegalArgumentException("INTERSECT of record sets not possible; columns of the two record sets are not the same!")
        }

        val records = this.map.keys.intersect(other.map.keys)
        return Recordset(this.columns).also {new ->
            records.forEach {
                if (this.map.containsKey(it)) {
                    new.addRow(it, this.map[it]!!)
                } else {
                    new.addRow(it, other.map[it]!!)
                }

            }
        }
    }

    /**
     * Returns the first [Record] in this [Recordset].
     *
     * @return The first [Record] of this [Recordset]
     */
    fun first(): Record? = this.map[this.map.firstKey()]

    /**
     * Applies the provided action to each [Record] in this [Recordset].
     *
     * @param action The action that should be applied.
     */
    @Synchronized
    override fun forEach(action: (Record) -> Unit) = this.map.values.forEach(action)

    /**
     * Applies the provided action to each [Record] in this [Recordset].
     *
     * @param action The action that should be applied.
     */
    @Synchronized
    fun forEachIndexed(action: (Int, Record) -> Unit) = this.map.values.forEachIndexed(action)

    /**
     * Applies the provided mapping function to each [Record] in this [Recordset].
     *
     * @param action The mapping function that should be applied.
     */
    @Synchronized
    override fun <R> map(action: (Record) -> R): Collection<R> = this.map.values.map(action)

    /**
     * Checks if this [Filterable] can process the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean = when {
        predicate is BooleanPredicate && predicate.atomics.all { it.operator != ComparisonOperator.LIKE } -> true
        else -> false
    }

    /**
     * Filters this [Filterable] thereby creating and returning a new [Filterable].
     *
     * @param predicate [Predicate] to filter [Record]s.
     * @return New [Filterable]
     */
    @Synchronized
    override fun filter(predicate: Predicate): Recordset {
        if (predicate is BooleanPredicate) {
            val recordset = Recordset(this.columns)
            this.map.values.asSequence().filter { predicate.matches(it) }.forEach { recordset.addRow(it) }
            return recordset
        } else {
            throw QueryException.UnsupportedPredicateException("Only boolean predicates are supported for invocation of filter() on a Recordset.")
        }
    }

    /**
     * Applies the provided action to each [Record] that matches the given [Predicate].
     *
     * @param action The action that should be applied.
     */
    @Synchronized
    override fun forEach(predicate: Predicate, action: (Record) -> Unit) {
        if (predicate is BooleanPredicate) {
            this.map.values.asSequence().filter { predicate.matches(it) }.forEach { action(it) }
        } else {
            throw QueryException.UnsupportedPredicateException("Only boolean predicates are supported for invocation of forEach() on a Recordset.")
        }
    }

    /**
     * Applies the provided mapping function to each [Record] that matches the given [Predicate].
     *
     * @param predicate [Predicate] to filter [Record]s.
     * @param action The mapping function that should be applied.
     */
    @Synchronized
    override fun <R> map(predicate: Predicate, action: (Record) -> R): Collection<R> {
        if (predicate is BooleanPredicate) {
            val list = mutableListOf<R>()
            this.map.values.asSequence().filter { predicate.matches(it) }.map(action).forEach { list.add(it) }
            return list
        } else {
            throw QueryException.UnsupportedPredicateException("Only boolean predicates are supported for invocation of map() on a Recordset.")
        }
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
     * Drops the columns with the specified indexes and returns a new [Recordset].
     *
     * @param columns A list of column indexes to drop.
     * @return A new [Recordset] without the specified [ColumnDef]s
     */
    fun dropColumnsWithIndex(columns: Collection<Int>): Recordset {
        val recordset = Recordset(this.columns.filterIndexed { i, _ -> !columns.contains(i) }.toTypedArray())
        this.map.forEach{(tid, r) ->
            recordset.addRowUnsafe(tid, r.values.filterIndexed {i,_ -> !columns.contains(i) }.toTypedArray())
        }
        return recordset
    }

    /**
     * Drops the columns with the specified indexes and returns a new [Recordset].
     *
     * @param columns A list of columns to drop.
     * @return A new [Recordset] without the specified [ColumnDef]s
     */
    fun dropColumns(columns: Collection<ColumnDef<*>>): Recordset = this.dropColumnsWithIndex(columns.map { this.indexOf(it) })

    /**
     *
     */
    fun renameColumnsWithIndex(columns: Collection<Pair<Int,String>>): Recordset {
        val renamed = this.columns.mapIndexed { i, col ->
            val rename = columns.find { i == it.first }
            if (rename != null) {
                ColumnDef(rename.second, col.type, col.size, col.nullable)
            } else {
                col
            }
        }.toTypedArray()

        val recordset = Recordset(renamed)
        this.forEach { r -> recordset.addRowUnsafe(r.tupleId, r.values) }
        return recordset
    }


    /**
     * Returns a list view of this [Recordset]
     *
     * @return The [List] underpinning this [Recordset].
     */
    @Synchronized
    fun toList(): List<Record> = this.map.values.toList()

    /**
     * Returns an [Iterator] of this [Recordset]
     *
     * @return [Iterator] of this [Recordset].
     */
    @Synchronized
    fun iterator(): Iterator<Record> = this.map.values.iterator()

    /**
     * A [Record] implementation that depends on the existence of the enclosing [Recordset].
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class RecordsetRecord(override val tupleId: Long, init: Array<Value<*>?>? = null) : Record {

        /** Array of [ColumnDef]s that describes the [Columns] of this [Record]. */
        override val columns: Array<out ColumnDef<*>>
            get() = this@Recordset.columns

        /** Array of column values (one entry per column). Initializes with null. */
        override val values: Array<Value<*>?> = if (init != null) {
            init.forEachIndexed { index, any -> columns[index].validateOrThrow(any) }
            init
        } else Array(columns.size) { columns[it].defaultValue() }

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
}