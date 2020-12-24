package org.vitrivr.cottontail.database.index.hash

import org.mapdb.DB
import org.mapdb.HTreeMap
import org.mapdb.Serializer
import org.slf4j.LoggerFactory
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
 * @author Ralph Gasser
 * @version 1.3.0
 */
class UniqueHashIndex(override val name: Name.IndexName, override val parent: Entity, override val columns: Array<ColumnDef<*>>) : Index() {

    /**
     * Index-wide constants.
     */
    companion object {
        const val MAP_FIELD_NAME = "map"
        private val LOGGER = LoggerFactory.getLogger(UniqueHashIndex::class.java)
    }

    /** Path to the [UniqueHashIndex] file. */
    override val path: Path = this.parent.path.resolve("idx_${name.simple}.db")

    /** The type of [Index] */
    override val type: IndexType = IndexType.HASH_UQ

    /** True since [UniqueHashIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** The [UniqueHashIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = this.columns

    /** The internal [DB] reference. */
    private val db: DB = this.parent.parent.parent.config.mapdb.db(this.path)

    /** Map structure used for [UniqueHashIndex]. */
    private val map: HTreeMap<out Value, TupleId> = this.db.hashMap(MAP_FIELD_NAME, this.columns.first().type.serializer(this.columns.size), Serializer.LONG_PACKED).createOrOpen()

    /**
     * Flag indicating if this [UniqueHashIndex] has been closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

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
    override fun canProcess(predicate: Predicate): Boolean = predicate is AtomicBooleanPredicate<*>
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
        predicate !is AtomicBooleanPredicate<*> || predicate.columns.first() != this.columns[0] || predicate.not -> Cost.INVALID
        predicate.operator == ComparisonOperator.EQUAL -> Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS, predicate.columns.map { it.physicalSize }.sum().toFloat())
        predicate.operator == ComparisonOperator.IN -> Cost(Cost.COST_DISK_ACCESS_READ * predicate.values.size, Cost.COST_MEMORY_ACCESS * predicate.values.size, predicate.columns.map { it.physicalSize }.sum().toFloat())
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
     * An [IndexTx] that affects this [UniqueHashIndex].
     *
     * @author Ralph Gasser
     * @version 1.3.0
     */
    private inner class Tx(context: TransactionContext) : Index.Tx(context) {

        /**
         * (Re-)builds the [UniqueHashIndex].
         **/
        override fun rebuild() = this.withWriteLock {
            LOGGER.trace("Rebuilding unique hash index {}", this@UniqueHashIndex.name)

            /* Clear existing map. */
            this@UniqueHashIndex.map.clear()

            /* (Re-)create index entries. */
            val localMap = this@UniqueHashIndex.map as HTreeMap<Value, TupleId>
            val txn = this.context.getTx(this.dbo.parent) as EntityTx
            txn.scan(this@UniqueHashIndex.columns).use { s ->
                s.forEach { record ->
                    val value = record[this.columns[0]]
                            ?: throw TxException.TxValidationException(this.context.txId, "A value cannot be null for instances of unique hash-index but tuple ${record.tupleId} is.")
                    if (!localMap.containsKey(value)) {
                        localMap[value] = record.tupleId
                    } else {
                        throw TxException.TxValidationException(this.context.txId, "Value $value must be unique for instances of unique hash-index but is not.")
                    }
                }
            }

            LOGGER.trace("Rebuilding unique hash index complete!")
        }

        /**
         * Updates the [UniqueHashIndex] with the provided [DataChangeEvent]s. This method determines,
         * whether the [Record] affected by the [DataChangeEvent] should be added or updated
         *
         * @param update Collection of [DataChangeEvent]s to process.
         */
        override fun update(update: Collection<DataChangeEvent>) = this.withWriteLock {
            val localMap = this@UniqueHashIndex.map as HTreeMap<Value, TupleId>

            /* Define action for inserting an entry based on a DataChangeEvent. */
            fun atomicInsert(event: DataChangeEvent) {
                val newValue = event.new?.get(this.columns[0])
                        ?: throw TxException.TxValidationException(this.context.txId, "A value cannot be null for instances of unique hash-index but tuple ${event.new?.tupleId} is.")
                localMap[newValue] = event.new.tupleId
            }

            /* Define action for deleting an entry based on a DataChangeEvent. */
            fun atomicDelete(event: DataChangeEvent) {
                val oldValue = event.old?.get(this.columns[0])
                        ?: throw TxException.TxValidationException(this.context.txId, "A value cannot be null for instances of unique hash-index but tuple ${event.new?.tupleId} is.")
                localMap.remove(oldValue)
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
         * Performs a lookup through this [UniqueHashIndex.Tx] and returns a [CloseableIterator] of
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
        override fun filter(predicate: Predicate) = object : CloseableIterator<Record> {

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
