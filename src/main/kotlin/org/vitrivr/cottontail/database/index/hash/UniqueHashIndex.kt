package org.vitrivr.cottontail.database.index.hash

import org.mapdb.DB
import org.mapdb.HTreeMap
import org.mapdb.Serializer
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.index.*
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.exceptions.TxException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Path
import java.util.*

/**
 * Represents an index in the Cottontail DB data model, that uses a persistent [HashMap] to map a
 * unique [Value] to a [TupleId]. Well suited for equality based lookups of [Value]s.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class UniqueHashIndex(
    override val name: Name.IndexName,
    override val parent: Entity,
    override val columns: Array<ColumnDef<*>>,
    override val path: Path
) : Index() {

    /**
     * Index-wide constants.
     */
    companion object {
        const val UQ_INDEX_MAP = "uq_map"
    }

    /** The type of [Index] */
    override val type: IndexType = IndexType.HASH_UQ

    /** The [UniqueHashIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = this.columns

    /** The internal [DB] reference. */
    private val db: DB = this.parent.parent.parent.config.mapdb.db(this.path)

    /** Map structure used for [UniqueHashIndex]. */
    private val map: HTreeMap<Value, TupleId> =
        this.db.hashMap(
            UQ_INDEX_MAP,
            this.columns.first().type.serializer(),
            Serializer.LONG_PACKED
        )
            .createOrOpen() as HTreeMap<Value, TupleId>

    /**
     * Flag indicating if this [UniqueHashIndex] has been closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    /** True since [UniqueHashIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** False, since [UniqueHashIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    /** Always false, due to incremental updating being supported. */
    override val dirty: Boolean = false

    init {
        this.db.commit() /* Initial commit. */
    }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [UniqueHashIndex]. [UniqueHashIndex] can be used to process IN and EQUALS
     * comparison operations on the specified column
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean = predicate is BooleanPredicate.Atomic
            && !predicate.not
            && predicate.columns.first() == this.columns[0]
            && (predicate.operator == ComparisonOperator.IN || predicate.operator == ComparisonOperator.EQUAL)

    /**
     * Calculates the cost estimate of this [UniqueHashIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost = when {
        predicate !is BooleanPredicate.Atomic || predicate.columns.first() != this.columns[0] || predicate.not -> Cost.INVALID
        predicate.operator == ComparisonOperator.EQUAL -> Cost(
            Cost.COST_DISK_ACCESS_READ,
            Cost.COST_MEMORY_ACCESS,
            predicate.columns.map { it.type.physicalSize }.sum().toFloat()
        )
        predicate.operator == ComparisonOperator.IN -> Cost(
            Cost.COST_DISK_ACCESS_READ * predicate.values.size,
            Cost.COST_MEMORY_ACCESS * predicate.values.size,
            predicate.columns.map { it.type.physicalSize }.sum().toFloat()
        )
        else -> Cost.INVALID
    }

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [Index].
     *
     * @param context [TransactionContext] to open the [Index.Tx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * Closes this [UniqueHashIndex] and the associated data structures.
     */
    override fun close() = this.closeLock.write {
        if (!this.closed) {
            this.db.close()
            this.closed = true
        }
    }

    /**
     * Adds a mapping from the given [Value] to the given [TupleId].
     *
     * @param key The [Value] key to add a mapping for.
     * @param tupleId The [TupleId] for the mapping.
     *
     * This is an internal function and can be used safely with values o
     */
    private fun addMapping(key: Value, tupleId: TupleId): Boolean {
        if (!this.columns[0].validate(key)) return false
        return this.map.putIfAbsentBoolean(key, tupleId)
    }

    /**
     * Removes a mapping from the given [Value] to the given [TupleId].
     *
     * @param key The [Value] key to remove a mapping for.
     *
     * This is an internal function and can be used safely with values o
     */
    private fun removeMapping(key: Value): Boolean {
        if (!this.columns[0].validate(key)) return false
        return this.map.remove(key) != null
    }

    /**
     * An [IndexTx] that affects this [UniqueHashIndex].
     */
    private inner class Tx(context: TransactionContext) : Index.Tx(context) {
        /**
         * Returns the number of entries in this [UniqueHashIndex.map] which should correspond
         * to the number of [TupleId]s it encods.
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
            entityTx.scan(this@UniqueHashIndex.columns).use { s ->
                s.forEach { record ->
                    val value = record[this.columns[0]]
                        ?: throw TxException.TxValidationException(
                            this.context.txId,
                            "Value cannot be null for UniqueHashIndex ${this@UniqueHashIndex.name} given value is (value = null, tupleId = ${record.tupleId})."
                        )
                    if (!this@UniqueHashIndex.addMapping(value, record.tupleId)) {
                        throw TxException.TxValidationException(
                            this.context.txId,
                            "Value must be unique for UniqueHashIndex ${this@UniqueHashIndex.name} but is not (value = $value, tupleId = ${record.tupleId})."
                        )
                    }
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
                    val value = event.inserts[this.columns[0]]
                    if (value != null) {
                        this@UniqueHashIndex.addMapping(value, event.tupleId)
                    }
                }
                is DataChangeEvent.UpdateDataChangeEvent -> {
                    val old = event.updates[this.columns[0]]?.first
                    if (old != null) {
                        this@UniqueHashIndex.removeMapping(old)
                    }
                    val new = event.updates[this.columns[0]]?.second
                    if (new != null) {
                        this@UniqueHashIndex.addMapping(new, event.tupleId)
                    }
                }
                is DataChangeEvent.DeleteDataChangeEvent -> {
                    val old = event.deleted[this.columns[0]]
                    if (old != null) {
                        this@UniqueHashIndex.removeMapping(old)
                    }
                }
            }
        }

        /**
         * Performs a lookup through this [UniqueHashIndex.Tx] and returns a [CloseableIterator] of
         * all [Record]s that match the [Predicate]. Only supports [BooleanPredicate.Atomic]s.
         *
         * The [CloseableIterator] is not thread safe!
         *
         * <strong>Important:</strong> It remains to the caller to close the [CloseableIterator]
         *
         * @param predicate The [Predicate] for the lookup
         *
         * @return The resulting [CloseableIterator]
         */
        override fun filter(predicate: Predicate) = object : CloseableIterator<Record> {

            /** Local [BooleanPredicate.Atomic] instance. */
            private val predicate: BooleanPredicate.Atomic

            /* Perform initial sanity checks. */
            init {
                require(predicate is BooleanPredicate.Atomic) { "UniqueHashIndex.filter() does only support AtomicBooleanPredicates." }
                require(!predicate.not) { "UniqueHashIndex.filter() does not support negated statements (i.e. NOT EQUALS or NOT IN)." }
                this@Tx.withReadLock { /* No op. */ }
                this.predicate = predicate
            }

            /** Flag indicating whether this [CloseableIterator] has been closed. */
            @Volatile
            override var isOpen = true
                private set

            /** Pre-fetched [Record]s that match the [Predicate]. */
            private val elements = LinkedList(this.predicate.values)

            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean {
                check(this.isOpen) { "Illegal invocation of hasNext(): This CloseableIterator has been closed." }
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
                check(this.isOpen) { "Illegal invocation of next(): This CloseableIterator has been closed." }
                val value = this.elements.poll()
                val tid = this@UniqueHashIndex.map[value]!!
                return StandaloneRecord(tid, this@UniqueHashIndex.produces, arrayOf(value))
            }

            /**
             * Closes this [CloseableIterator] and releases all locks and resources associated with it.
             */
            override fun close() {
                if (this.isOpen) {
                    this.isOpen = false
                }
            }
        }

        /**
         * The [UniqueHashIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] to perform the lookup.
         * @param range The [LongRange] to consider.
         * @return The resulting [CloseableIterator].
         */
        override fun filterRange(
            predicate: Predicate,
            range: LongRange
        ): CloseableIterator<Record> {
            throw UnsupportedOperationException("The UniqueHashIndex does not support ranged filtering!")
        }

        /** Performs the actual COMMIT operation by rolling back the [IndexTx]. */
        override fun performCommit() {
            this@UniqueHashIndex.db.commit()
        }

        /** Performs the actual ROLLBACK operation by rolling back the [IndexTx]. */
        override fun performRollback() {
            this@UniqueHashIndex.db.rollback()
        }
    }
}
