package org.vitrivr.cottontail.database.index.hash

import org.mapdb.HTreeMap
import org.mapdb.Serializer
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.index.AbstractIndex
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.exceptions.TxException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value
import java.nio.file.Path
import java.util.*

/**
 * Represents an index in the Cottontail DB data model, that uses a persistent [HashMap] to map a
 * unique [Value] to a [TupleId]. Well suited for equality based lookups of [Value]s.
 *
 * @author Ralph Gasser
 * @version 2.0.1
 */
class UniqueHashIndex(path: Path, parent: DefaultEntity) : AbstractIndex(path, parent) {

    /** Index-wide constants. */
    companion object {
        const val UQ_INDEX_MAP = "cdb_uq_map"
    }

    /** The type of [AbstractIndex] */
    override val type: IndexType = IndexType.HASH_UQ

    /** The [UniqueHashIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = this.columns

    /** Map structure used for [UniqueHashIndex]. */
    private val map: HTreeMap<Value, TupleId> = this.store.hashMap(UQ_INDEX_MAP, this.columns[0].type.serializerFactory().mapdb(this.columns[0].type.logicalSize), Serializer.LONG_DELTA).createOrOpen() as HTreeMap<Value, TupleId>

    /** True since [UniqueHashIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** False, since [UniqueHashIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    init {
        this.store.commit() /* Initial commit. */
    }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [UniqueHashIndex]. [UniqueHashIndex] can be used to process IN and EQUALS
     * comparison operations on the specified column
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean = predicate is BooleanPredicate.Atomic.Literal
            && !predicate.not
            && predicate.columns.first() == this.columns[0]
            && (predicate.operator is ComparisonOperator.In || predicate.operator is ComparisonOperator.Binary.Equal)

    /**
     * Calculates the cost estimate of this [UniqueHashIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost = when {
        predicate !is BooleanPredicate.Atomic.Literal || predicate.columns.first() != this.columns[0] || predicate.not -> Cost.INVALID
        predicate.operator is ComparisonOperator.Binary.Equal -> Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS, predicate.columns.map { it.type.physicalSize }.sum().toFloat())
        predicate.operator is ComparisonOperator.In -> Cost(Cost.COST_DISK_ACCESS_READ * predicate.operator.right.size, Cost.COST_MEMORY_ACCESS * predicate.operator.right.size, predicate.columns.map { it.type.physicalSize }.sum().toFloat())
        else -> Cost.INVALID
    }

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [AbstractIndex].
     *
     * @param context [TransactionContext] to open the [AbstractIndex.Tx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * An [IndexTx] that affects this [UniqueHashIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {

        /**
         * Adds a mapping from the given [Value] to the given [TupleId].
         *
         * @param key The [Value] key to add a mapping for.
         * @param tupleId The [TupleId] for the mapping.
         *
         * This is an internal function and can be used safely with values o
         */
        private fun addMapping(key: Value, tupleId: TupleId): Boolean {
            return this@UniqueHashIndex.map.putIfAbsentBoolean(key, tupleId)
        }

        /**
         * Removes a mapping from the given [Value] to the given [TupleId].
         *
         * @param key The [Value] key to remove a mapping for.
         *
         * This is an internal function and can be used safely with values o
         */
        private fun removeMapping(key: Value): Boolean {
            return this@UniqueHashIndex.map.remove(key) != null
        }

        /**
         * Returns the number of entries in this [UniqueHashIndex.map] which should correspond
         * to the number of [TupleId]s it encodes.
         *
         * @return Number of [TupleId]s in this [UniqueHashIndex]
         */
        override fun count(): Long = this.withReadLock {
            this@UniqueHashIndex.map.count().toLong()
        }

        /**
         * (Re-)builds the [UniqueHashIndex].
         */
        override fun rebuild() = this.withWriteLock {
            /* Obtain Tx for parent [Entity. */
            val entityTx = this.context.getTx(this.dbo.parent) as EntityTx

            /* Recreate entries. */
            this@UniqueHashIndex.map.clear()
            entityTx.scan(this@UniqueHashIndex.columns).forEach { record ->
                val value = record[this.dbo.columns[0]] ?: throw TxException.TxValidationException(this.context.txId, "Value cannot be null for UniqueHashIndex ${this@UniqueHashIndex.name} given value is (value = null, tupleId = ${record.tupleId}).")
                if (!this.addMapping(value, record.tupleId)) {
                    throw TxException.TxValidationException(this.context.txId, "Value must be unique for UniqueHashIndex ${this@UniqueHashIndex.name} but is not (value = $value, tupleId = ${record.tupleId}).")
                }
            }
        }

        /**
         * Updates the [UniqueHashIndex] with the provided [DataChangeEvent]s. This method determines,
         * whether the [Record] affected by the [DataChangeEvent] should be added or updated
         *
         * @param event [DataChangeEvent]s to process.
         */
        override fun update(event: DataChangeEvent) = this.withWriteLock {
            when (event) {
                is DataChangeEvent.InsertDataChangeEvent -> {
                    val value = event.inserts[this.dbo.columns[0]]
                    if (value != null) {
                        this.addMapping(value, event.tupleId)
                    }
                }
                is DataChangeEvent.UpdateDataChangeEvent -> {
                    val old = event.updates[this.dbo.columns[0]]?.first
                    if (old != null) {
                        this.removeMapping(old)
                    }
                    val new = event.updates[this.dbo.columns[0]]?.second
                    if (new != null) {
                        this.addMapping(new, event.tupleId)
                    }
                }
                is DataChangeEvent.DeleteDataChangeEvent -> {
                    val old = event.deleted[this.dbo.columns[0]]
                    if (old != null) {
                        this.removeMapping(old)
                    }
                }
            }
        }

        /**
         * Clears the [UniqueHashIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.withWriteLock {
            this@UniqueHashIndex.dirtyField.compareAndSet(false, true)
            this@UniqueHashIndex.map.clear()
        }

        /**
         * Performs a lookup through this [UniqueHashIndex.Tx] and returns a [Iterator] of
         * all [Record]s that match the [Predicate]. Only supports [BooleanPredicate.Atomic]s.
         *
         * The [Iterator] is not thread safe!
         **
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate) = object : Iterator<Record> {

            /** Local [BooleanPredicate.Atomic] instance. */
            private val predicate: BooleanPredicate.Atomic.Literal

            /** Pre-fetched [Record]s that match the [Predicate]. */
            private val elements = LinkedList<Value>()

            /* Perform initial sanity checks. */
            init {
                require(predicate is BooleanPredicate.Atomic.Literal) { "UniqueHashIndex.filter() does only support Atomic.Literal boolean predicates." }
                require(!predicate.not) { "UniqueHashIndex.filter() does not support negated statements (i.e. NOT EQUALS or NOT IN)." }
                this@Tx.withReadLock { /* No op. */ }
                this.predicate = predicate
                when (predicate.operator) {
                    is ComparisonOperator.In -> this.elements.addAll(predicate.operator.right.map { it.value })
                    is ComparisonOperator.Binary.Equal -> this.elements.add(predicate.operator.right.value)
                    else -> throw IllegalArgumentException("UniqueHashIndex.filter() does only support EQUAL and IN operators.")
                }
            }

            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean {
                while (this.elements.isNotEmpty()) {
                    if (this@UniqueHashIndex.map.contains(this.elements.peek())) {
                        return true
                    } else {
                        this.elements.remove()
                    }
                }
                return false
            }

            /**
             * Returns the next element in the iteration.
             */
            override fun next(): Record {
                val value = this.elements.poll()
                val tid = this@UniqueHashIndex.map[value]!!
                return StandaloneRecord(tid, this@UniqueHashIndex.produces, arrayOf(value))
            }
        }

        /**
         * The [UniqueHashIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partitionIndex The [partitionIndex] for this [filterRange] call.
         * @param partitions The total number of partitions for this [filterRange] call.
         * @return The resulting [Iterator].
         */
        override fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int): Iterator<Record> {
            throw UnsupportedOperationException("The UniqueHashIndex does not support ranged filtering!")
        }
    }
}
