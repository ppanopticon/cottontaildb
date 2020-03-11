package ch.unibas.dmi.dbis.cottontail.model.recordset

import ch.unibas.dmi.dbis.cottontail.database.queries.*
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.model.basics.*
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.read
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList

import java.lang.IllegalArgumentException
import java.util.concurrent.locks.StampedLock

/**
 * A [Recordset] as returned and processed by Cottontail DB. [Recordset]s are tables. A [Recordset]'s columns are defined by the [ColumnDef]'s
 * it contains ([Recordset.columns] and it contains an arbitrary number of [Record] entries as rows.
 *
 * [Recordset]s are the unit of data retrieval and processing in Cottontail DB. Whenever information is accessed through an [Entity][ch.unibas.dmi.dbis.cottontail.database.entity.Entity],
 * a [Recordset] is being generated. Furthermore, the entire query execution pipeline processes, transforms and produces [Recordset]s.
 *
 * @see ch.unibas.dmi.dbis.cottontail.database.entity.Entity
 * @see QueryExecutionTask
 *
 * @author Ralph Gasser
 * @version 1.3
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
     * Creates a new [Record] and appends it to this [Recordset]. This is a potentially unsafe operation.
     *
     * @param values The values to add to this [Recordset].
     */
    fun addRowUnsafe(values: Array<Value?>) = this.lock.write {
        this.list.add(RecordsetRecord(this.list.size64()).assign(values))
    }

    /**
     * Creates a new [Record] given the provided tupleId and values and appends it to this [Recordset].
     * This is a potentially unsafe operation.
     *
     * @param tupleId The tupleId of the new [Record].
     * @param values The values to add to this [Recordset].
     */
    fun addRowUnsafe(tupleId: Long, values: Array<Value?>) = this.lock.write {
        this.list.add(RecordsetRecord(tupleId).assign(values))
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
    fun addRowIfUnsafe(tupleId: Long, predicate: BooleanPredicate, values: Array<Value?>): Boolean = this.lock.write {
        val record = RecordsetRecord(tupleId).assign(values)
        return if (predicate.matches(record)) {
            this.list.add(record)
            true
        } else {
            false
        }
    }

    /**
     * Appends a [Record] (without a tupleId) to this [Recordset].
     *
     * @param record The record to add to this [Recordset].
     */
    fun addRow(record: Record) = this.lock.write {
        if (record.columns.contentDeepEquals(this.columns)) {
            this.list.add(RecordsetRecord(record.tupleId).assign(record.values))
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
     * @param record The values to add to this [Recordset].
     * @return True if [Record] was added, false otherwise.
     */
    fun addRowIf(predicate: BooleanPredicate, record: Record): Boolean = this.lock.write {
        if (record.columns.contentDeepEquals(this.columns)) {
            return if (predicate.matches(record)) {
                this.list.add(RecordsetRecord(record.tupleId).assign(record.values))
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
    operator fun get(tupleId: Long): Record {
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
        return Recordset(this.columns).also {new ->
            this.forEach {
                new.addRow(it)
            }
            other.forEach {
                new.addRow(it)
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
        return Recordset(this.columns).also {new ->
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
    override fun forEach(action: (Record) -> Unit) = this.lock.read {
        this.list.forEach(action)
    }

    /**
     * Applies the provided action to each [Record] in this [Recordset].
     *
     * @param from The tuple ID of the first [Record] to iterate over.
     * @param to The tuple ID of the last [Record] to iterate over.
     * @param action The action that should be applied.
     */
    override fun forEach(from: Long, to: Long, action: (Record) -> Unit) = this.lock.read {
        if (from >= this.list.size64() || to >= this.list.size64()) throw ArrayIndexOutOfBoundsException("Range [$from, $to] is out of bounds for Recordset with size ${this.list.size64()}.")
        (from until to).forEach {
            action(this.list[it])
        }
    }

    /**
     * Applies the provided action to each [Record] in this [Recordset].
     *
     * @param action The action that should be applied.
     */
    fun forEachIndexed(action: (Int, Record) -> Unit) = this.lock.read {
        this.list.forEachIndexed(action)
    }

    /**
     * Applies the provided mapping function to each [Record] in this [Recordset].
     *
     * @param action The mapping function that should be applied.
     */
    override fun <R> map(action: (Record) -> R): Collection<R> = this.lock.read {
        this.list.map(action)
    }


    /**
     * Applies the provided mapping function to each [Record] in this [Recordset].
     *
     * @param from The tuple ID of the first [Record] to iterate over.
     * @param to The tuple ID of the last [Record] to iterate over.
     * @param action The mapping function that should be applied.
     */
    override fun <R> map(from: Long, to: Long, action: (Record) -> R): Collection<R> = this.lock.read {
        if (from >= this.list.size64() || to >= this.list.size64()) throw ArrayIndexOutOfBoundsException("Range [$from, $to] is out of bounds for Recordset with size ${this.list.size64()}.")
        (from until to).map {
            action(this.list[it])
        }
    }

    /**
     * Checks if this [Filterable] can process the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean = predicate is BooleanPredicate

    /**
     * Filters this [Filterable] thereby creating and returning a new [Filterable].
     *
     * @param predicate [Predicate] to filter [Record]s.
     * @return New [Filterable]
     */
    override fun filter(predicate: Predicate): Recordset = if (predicate is BooleanPredicate) {
        val recordset = Recordset(this.columns)
        this.lock.read {
            this.list.asSequence().filter { predicate.matches(it) }.forEach { recordset.addRow(it) }
        }
        recordset
    } else {
        throw QueryException.UnsupportedPredicateException("Recordset#filter() does not support predicates of type '${predicate::class.simpleName}'.")
    }

    /**
     * Applies the provided action to each [Record] that matches the given [Predicate].
     *
     * @param predicate The [Predicate] to filter [Record]s.
     * @param action The action that should be applied.
     */
    override fun forEach(predicate: Predicate, action: (Record) -> Unit) = if (predicate is BooleanPredicate) {
        this.lock.read {
            this.list.asSequence().filter { predicate.matches(it) }.forEach { action(it) }
        }
    } else {
        throw QueryException.UnsupportedPredicateException("Recordset#forEach() does not support predicates of type '${predicate::class.simpleName}'.")
    }

    /**
     * Applies the provided action to each [Record] in the given range that matches the given [Predicate].
     *
     * @param from The tuple ID of the first [Record] to iterate over.
     * @param to The tuple ID of the last [Record] to iterate over.
     * @param predicate The [Predicate] to filter [Record]s.
     * @param action The action that should be applied.
     */
    override fun forEach(from: Long, to: Long, predicate: Predicate, action: (Record) -> Unit) = if (predicate is BooleanPredicate) {
        this.lock.read {
            if (from >= this.list.size64() || to >= this.list.size64()) throw ArrayIndexOutOfBoundsException("Range [$from, $to] is out of bounds for Recordset with size ${this.list.size64()}.")
            (from until to).asSequence().map { this.list[it] }.filter { predicate.matches(it) }.forEach { action(it) }
        }
    } else {
        throw QueryException.UnsupportedPredicateException("Recordset#forEach() does not support predicates of type '${predicate::class.simpleName}'.")
    }


    /**
     * Applies the provided mapping function to each [Record] that matches the given [Predicate].
     *
     * @param predicate The [Predicate] to filter [Record]s.
     * @param action The mapping function that should be applied.
     */
    override fun <R> map(predicate: Predicate, action: (Record) -> R): Collection<R> = if (predicate is BooleanPredicate) {
        this.lock.read {
            this.list.asSequence().filter { predicate.matches(it) }.map(action).toList()
        }
    } else {
        throw QueryException.UnsupportedPredicateException("Recordset#map() does not support predicates of type '${predicate::class.simpleName}'.")
    }

    /**
     * Applies the provided mapping function to each [Record] in the given range that matches the given [Predicate].
     *
     * @param from The tuple ID of the first [Record] to iterate over.
     * @param to The tuple ID of the last [Record] to iterate over.
     * @param predicate The [Predicate] to filter [Record]s.
     * @param action The mapping function that should be applied.
     */
    override fun <R> map(from: Long, to: Long, predicate: Predicate, action: (Record) -> R): Collection<R> = if (predicate is BooleanPredicate) {
        this.lock.read {
            if (from >= this.list.size64() || to >= this.list.size64()) throw ArrayIndexOutOfBoundsException("Range [$from, $to] is out of bounds for Recordset with size ${this.list.size64()}.")
            (from until to).asSequence().map { this.list[it] }.filter { predicate.matches(it) }.map(action).toList()
        }
    } else {
        throw QueryException.UnsupportedPredicateException("Recordset#map() does not support predicates of type '${predicate::class.simpleName}'.")
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
    fun dropColumnsWithIndex(columns: Collection<Int>): Recordset = this.lock.write {
        val recordset = Recordset(this.columns.filterIndexed { i, _ -> !columns.contains(i) }.toTypedArray())
        this.list.forEach{
            recordset.addRowUnsafe(it.tupleId, it.values.filterIndexed {i, _ -> !columns.contains(i) }.toTypedArray())
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
     * Renames the columns with the specified indexes and returns a new [Recordset].
     *
     * @param columns A list of columns to drop.
     * @return A new [Recordset] without the specified [ColumnDef]s
     */
    fun renameColumnsWithIndex(columns: Collection<Pair<Int, Name>>): Recordset {
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
    inner class RecordsetRecord(override val tupleId: Long, init: Array<Value?>? = null) : Record {

        /** Array of [ColumnDef]s that describes the [Column][ch.unibas.dmi.dbis.cottontail.database.column.Column] of this [Record]. */
        override val columns: Array<ColumnDef<*>>
            get() = this@Recordset.columns


        /** Array of column values (one entry per column). Initializes with null. */
        override val values: Array<Value?> = if (init != null) {
            assert(init.size == columns.size)
            init.forEachIndexed { index, any -> columns[index].validateOrThrow(any) }
            init
        } else Array(columns.size) { columns[it].defaultValue() }

        /**
         * Copies this [Record] and returns the copy.
         *
         * @return Copy of this [Record]
         */
        override fun copy(): Record = StandaloneRecord(tupleId, columns = columns, init = values.copyOf())

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