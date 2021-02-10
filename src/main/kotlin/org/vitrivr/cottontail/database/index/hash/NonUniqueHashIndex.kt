package org.vitrivr.cottontail.database.index.hash

import org.mapdb.DB
import org.mapdb.Serializer
import org.mapdb.serializer.SerializerArrayTuple
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
 * Represents an [Index] in the Cottontail DB data model, that uses a persistent [HashMap]
 * to map a [Value] to a [TupleId]. Well suited for equality based lookups of [Value]s.
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 1.5.0
 */
class NonUniqueHashIndex(
    override val name: Name.IndexName,
    override val parent: Entity,
    override val columns: Array<ColumnDef<*>>,
    override val path: Path
) : Index() {
    /**
     * Index-wide constants.
     */
    companion object {
        const val NUQ_INDEX_MAP = "nuq_map"
    }

    /** The type of [Index] */
    override val type: IndexType = IndexType.HASH

    /** True since [NonUniqueHashIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** False, since [NonUniqueHashIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    /** Always false, due to incremental updating being supported. */
    override val dirty: Boolean = false

    /** The [NonUniqueHashIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = this.columns

    /** The internal [DB] reference. */
    private val db: DB = this.parent.parent.parent.config.mapdb.db(this.path)

    /** Map structure used for [NonUniqueHashIndex]. */
    private val map: NavigableSet<Array<Any>> =
        this.db.treeSet(
            NUQ_INDEX_MAP,
            SerializerArrayTuple(this.columns.first().type.serializer(), Serializer.LONG_DELTA)
        )
            .createOrOpen()

    /**
     * Flag indicating if this [NonUniqueHashIndex] has been closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    init {
        this.db.commit() /* Initial commit. */
    }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [NonUniqueHashIndex]. [NonUniqueHashIndex] can be used to process IN and EQUALS
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
     * Calculates the cost estimate of this [NonUniqueHashIndex] processing the provided [Predicate].
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
     * @param context If the [TransactionContext] to create the [IndexTx] for..
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * Closes this [NonUniqueHashIndex] and the associated data structures.
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
        this.map.add(arrayOf(key, tupleId))
        return true
    }

    /**
     * Removes a mapping from the given [Value] to the given [TupleId].
     *
     * @param key The [Value] key to remove a mapping for.
     * @param tupleId The [TupleId] to remove.
     *
     * This is an internal function and can be used safely with values o
     */
    private fun removeMapping(key: Value, tupleId: TupleId): Boolean {
        if (!this.columns[0].validate(key)) return false
        return this.map.remove(arrayOf(key, tupleId))
    }

    /**
     * An [IndexTx] that affects this [NonUniqueHashIndex].
     */
    private inner class Tx(context: TransactionContext) : Index.Tx(context) {

        /**
         * Returns the number of entries in this [NonUniqueHashIndex.map] which DOES NOT directly
         * correspond to the number of [TupleId]s it encodes.
         *
         * @return Number of [TupleId]s in this [NonUniqueHashIndex]
         */
        override fun count(): Long = this.withReadLock {
            this@NonUniqueHashIndex.map.size.toLong()
        }

        /**
         * (Re-)builds the [NonUniqueHashIndex].
         */
        override fun rebuild() = this.withWriteLock {
            /* Obtain Tx for parent [Entity. */
            val entityTx = this.context.getTx(this.dbo.parent) as EntityTx

            /* Recreate entries. */
            this@NonUniqueHashIndex.map.clear()
            entityTx.scan(this.columns).use { s ->
                s.forEach { record ->
                    val value = record[this.columns[0]] ?: throw TxException.TxValidationException(
                        this.context.txId,
                        "A value cannot be null for instances of NonUniqueHashIndex ${this@NonUniqueHashIndex.name} but given value is (value = null, tupleId = ${record.tupleId})."
                    )
                    this@NonUniqueHashIndex.addMapping(value, record.tupleId)
                }
            }
        }

        /**
         * Updates the [NonUniqueHashIndex] with the provided [Record]. This method determines, whether
         * the [Record] affected by the [DataChangeEvent] should be added or updated
         *
         * @param event [DataChangeEvent] to process.
         */
        override fun update(event: DataChangeEvent) = this.withWriteLock {
            when (event) {
                is DataChangeEvent.InsertDataChangeEvent -> {
                    val value = event.inserts[this.columns[0]]
                    if (value != null) {
                        this@NonUniqueHashIndex.addMapping(value, event.tupleId)
                    }
                }
                is DataChangeEvent.UpdateDataChangeEvent -> {
                    val old = event.updates[this.columns[0]]?.first
                    if (old != null) {
                        this@NonUniqueHashIndex.removeMapping(old, event.tupleId)
                    }
                    val new = event.updates[this.columns[0]]?.second
                    if (new != null) {
                        this@NonUniqueHashIndex.addMapping(new, event.tupleId)
                    }
                }
                is DataChangeEvent.DeleteDataChangeEvent -> {
                    val old = event.deleted[this.columns[0]]
                    if (old != null) {
                        this@NonUniqueHashIndex.removeMapping(old, event.tupleId)
                    }
                }
            }
        }

        /**
         * Performs a lookup through this [NonUniqueHashIndex.Tx] and returns a [CloseableIterator] of
         * all [Record]s that match the [Predicate]. Only supports [ BooleanPredicate.AtomicBooleanPredicate]s.
         *
         * The [CloseableIterator] is not thread safe!
         *
         * <strong>Important:</strong> It remains to the caller to close the [CloseableIterator]
         *
         * @param predicate The [Predicate] for the lookup
         *
         * @return The resulting [CloseableIterator]
         */
        override fun filter(predicate: Predicate): CloseableIterator<Record> = object : CloseableIterator<Record> {

            /** Local [ BooleanPredicate.AtomicBooleanPredicate] instance. */
            private val predicate: BooleanPredicate.Atomic

            /* Perform initial sanity checks. */
            init {
                require(predicate is BooleanPredicate.Atomic) { "NonUniqueHashIndex.filter() does only support AtomicBooleanPredicates." }
                require(!predicate.not) { "NonUniqueHashIndex.filter() does not support negated statements (i.e. NOT EQUALS or NOT IN)." }
                this@Tx.withReadLock { /* No op. */ }
                this.predicate = predicate
            }

            /** Flag indicating whether this [CloseableIterator] has been closed. */
            @Volatile
            private var closed = false

            /** Pre-fetched entries that match the [Predicate]. */
            private val elements = LinkedList<Array<Any>>()

            init {
                if (this.predicate.operator == ComparisonOperator.IN) {
                    this.predicate.values.forEach { v ->
                        val subset = this@NonUniqueHashIndex.map.subSet(
                            arrayOf(v),
                            arrayOf(v, (null as Any?)) as Array<Any> /* Safe! */
                        )
                        this.elements.addAll(subset)
                    }
                } else if (this.predicate.operator == ComparisonOperator.EQUAL) {
                    val v = this.predicate.values.first()
                    val subset = this@NonUniqueHashIndex.map.subSet(
                        arrayOf(v),
                        arrayOf(v, (null as Any?)) as Array<Any> /* Safe! */
                    )
                    this.elements.addAll(subset)
                }
            }

            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean {
                check(!this.closed) { "Illegal invocation of hasNext(): This CloseableIterator has been closed." }
                return this.elements.isNotEmpty()
            }

            /**
             * Returns the next element in the iteration.
             */
            override fun next(): Record {
                check(!this.closed) { "Illegal invocation of next(): This CloseableIterator has been closed." }
                val next = this.elements.poll()
                return StandaloneRecord(
                    next[1] as TupleId,
                    this@Tx.columns,
                    arrayOf(next[0] as Value)
                )
            }

            /**
             * Closes this [CloseableIterator] and releases all locks and resources associated with it.
             */
            override fun close() {
                if (!this.closed) {
                    this.closed = true
                }
            }
        }

        /**
         * The [NonUniqueHashIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] to perform the lookup.
         * @param range The [LongRange] to consider.
         * @return The resulting [CloseableIterator].
         */
        override fun filterRange(
            predicate: Predicate,
            range: LongRange
        ): CloseableIterator<Record> {
            throw UnsupportedOperationException("The NonUniqueHashIndex does not support ranged filtering!")
        }

        /** Performs the actual COMMIT operation by rolling back the [IndexTx]. */
        override fun performCommit() {
            this@NonUniqueHashIndex.db.commit()
        }

        /** Performs the actual ROLLBACK operation by rolling back the [IndexTx]. */
        override fun performRollback() {
            this@NonUniqueHashIndex.db.rollback()
        }
    }
}
