package org.vitrivr.cottontail.database.index.hash

import org.mapdb.DB
import org.mapdb.HTreeMap
import org.mapdb.Serializer
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.events.DataChangeEventType
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.components.AtomicBooleanPredicate
import org.vitrivr.cottontail.database.queries.components.ComparisonOperator
import org.vitrivr.cottontail.database.queries.components.Predicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.exceptions.TxException
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Path
import java.util.*

/**
 * Represents an index in the Cottontail DB data model. An [Index] belongs to an [Entity] and can be used to index one to many
 * [Column]s. Usually, [Index]es allow for faster data access. They process [Predicate]s and return [Recordset]s.
 *
 * @see Schema
 * @see Column
 * @see Entity.Tx
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 1.3.0
 */
class NonUniqueHashIndex(override val name: Name.IndexName, override val parent: Entity, override val columns: Array<ColumnDef<*>>) : Index() {
    /**
     * Index-wide constants.
     */
    companion object {
        const val MAP_FIELD_NAME = "nu_map"
    }

    /** Path to the [NonUniqueHashIndex] file. */
    override val path: Path = this.parent.path.resolve("idx_nu_${name.simple}.db")

    /** The type of [Index] */
    override val type: IndexType = IndexType.HASH_UQ

    /** True since [NonUniqueHashIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** The [NonUniqueHashIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = this.columns

    /** The internal [DB] reference. */
    private val db: DB = this.parent.parent.parent.config.mapdb.db(this.path)

    /** Map structure used for [NonUniqueHashIndex]. */
    private val map: HTreeMap<out Value, LongArray> = this.db.hashMap(MAP_FIELD_NAME, this.columns.first().type.serializer(this.columns.size), Serializer.LONG_ARRAY).createOrOpen()

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
    override fun canProcess(predicate: Predicate): Boolean = predicate is AtomicBooleanPredicate<*>
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
        predicate !is AtomicBooleanPredicate<*> || predicate.columns.first() != this.columns[0] || predicate.not -> Cost.INVALID
        predicate.operator == ComparisonOperator.EQUAL -> Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS, predicate.columns.map { it.physicalSize }.sum().toFloat())
        predicate.operator == ComparisonOperator.IN -> Cost(Cost.COST_DISK_ACCESS_READ * predicate.values.size, Cost.COST_MEMORY_ACCESS * predicate.values.size, predicate.columns.map { it.physicalSize }.sum().toFloat())
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
     * An [IndexTx] that affects this [NonUniqueHashIndex].
     *
     * @author Ralph Gasser
     * @version 1.3.0
     */
    private inner class Tx(context: TransactionContext) : Index.Tx(context) {
        /**
         * (Re-)builds the [NonUniqueHashIndex].
         */
        override fun rebuild() = this.withReadLock {
            /* Clear existing map. */
            this@NonUniqueHashIndex.map.clear()

            /* (Re-)create index entries. */
            val localMap = mutableMapOf<Value, MutableList<Long>>()

            /* Obtain Tx for parent [Entity. */
            val entityTx = this.context.getTx(this.dbo.parent) as EntityTx
            entityTx.scan(this.columns).use { s ->
                s.forEach { record ->
                    val value = record[this.columns[0]]
                            ?: throw TxException.TxValidationException(this.context.txId, "A value cannot be null for instances of non-unique hash-index but tuple ${record.tupleId} is.")
                    if (!localMap.containsKey(value)) {
                        localMap[value] = mutableListOf(record.tupleId)
                    } else {
                        localMap[value]!!.add(record.tupleId)
                    }
                }
            }
            val castMap = this@NonUniqueHashIndex.map as HTreeMap<Value, LongArray>
            localMap.forEach { (value, l) -> castMap[value] = l.toLongArray() }
        }

        /**
         * Updates the [NonUniqueHashIndex] with the provided [Record]. This method determines, whether
         * the [Record] affected by the [DataChangeEvent] should be added or updated
         *
         * @param update Collection of [DataChangeEvent]s to process.
         */
        override fun update(update: Collection<DataChangeEvent>) = this.withWriteLock {
            val localMap = this@NonUniqueHashIndex.map as HTreeMap<Value, LongArray>

            /* Inner function for inserting an entry based on a DataChangeEvent. */
            fun atomicInsert(event: DataChangeEvent) {
                val newValue = event.new?.get(this.columns[0])
                        ?: throw TxException.TxValidationException(this.context.txId, "A value cannot be null for instances of non-unique hash-index but tuple ${event.new?.tupleId} is.")
                if (localMap.containsKey(newValue)) {
                    val oldArray = localMap[newValue]!!
                    if (!oldArray.contains(event.new.tupleId)) {
                        val newArray = oldArray.copyOf(oldArray.size + 1)
                        newArray[oldArray.size] = event.new.tupleId
                        localMap[newValue] = newArray
                    }
                } else {
                    localMap[newValue] = longArrayOf(event.new.tupleId)
                }
            }

            /* Inner function for deleting an entry based on a DataChangeEvent. */
            fun atomicDelete(event: DataChangeEvent) {
                val oldValue = event.old?.get(this.columns[0])
                        ?: throw TxException.TxValidationException(this.context.txId, "A value cannot be null for instances of non-unique hash-index but tuple ${event.new?.tupleId} is.")
                if (localMap.containsKey(oldValue)) {
                    val oldArray = localMap[oldValue]!!
                    if (oldArray.contains(event.old.tupleId)) {
                        localMap[oldValue] = oldArray.filter { it != event.old.tupleId }.toLongArray()
                    }
                }
            }

            /* Process the DataChangeEvents. */
            loop@ for (event in update) {
                when (event.type) {
                    DataChangeEventType.INSERT -> atomicInsert(event)
                    DataChangeEventType.UPDATE -> {
                        if (event.new?.get(this.columns[0]) != event.old?.get(this.columns[0])) {
                            atomicDelete(event)
                            atomicInsert(event)
                        }
                    }
                    DataChangeEventType.DELETE -> atomicDelete(event)
                    else -> continue@loop
                }
            }
        }

        /**
         * Performs a lookup through this [NonUniqueHashIndex.Tx] and returns a [CloseableIterator] of
         * all [Record]s that match the [Predicate]. Only supports [AtomicBooleanPredicate]s.
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

            /** Local [AtomicBooleanPredicate] instance. */
            private val predicate: AtomicBooleanPredicate<*>

            /* Perform initial sanity checks. */
            init {
                require(predicate is AtomicBooleanPredicate<*>) { "NonUniqueHashIndex.filter() does only support AtomicBooleanPredicates." }
                require(!predicate.not) { "NonUniqueHashIndex.filter() does not support negated statements (i.e. NOT EQUALS or NOT IN)." }
                this@Tx.withReadLock { /* No op. */ }
                this.predicate = predicate
            }

            /** Flag indicating whether this [CloseableIterator] has been closed. */
            @Volatile
            override var isOpen = true
                private set

            /** Pre-fetched [Record]s that match the [Predicate]. */
            private val elements = LinkedList(this.predicate.values)

            /** The current [Value] inspected by this [NonUniqueHashIndex]. */
            private var current: Value? = null

            /** List of [TupleId]s ready for emission. */
            private var entries = LinkedList<TupleId>()

            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean {
                check(this.isOpen) { "Illegal invocation of hasNext(): This CloseableIterator has been closed." }
                if (this.entries.isNotEmpty()) {
                    return true
                } else {
                    while (this.elements.isNotEmpty()) {
                        this.current = this.elements.poll()
                        val next = this@NonUniqueHashIndex.map[this.current!!]
                        if (next != null) {
                            this.entries.clear()
                            next.forEach { this.entries.add(it) }
                            return true
                        }
                    }
                }
                return false
            }

            /**
             * Returns the next element in the iteration.
             */
            override fun next(): Record {
                check(this.isOpen) { "Illegal invocation of next(): This CloseableIterator has been closed." }
                return StandaloneRecord(this.entries.poll(), this@Tx.columns, arrayOf(this.current))
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
