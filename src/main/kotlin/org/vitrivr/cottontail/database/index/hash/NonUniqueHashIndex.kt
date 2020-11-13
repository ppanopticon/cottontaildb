package org.vitrivr.cottontail.database.index.hash

import org.mapdb.DB
import org.mapdb.HTreeMap
import org.mapdb.Serializer
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.events.DataChangeEventType
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTransaction
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.components.AtomicBooleanPredicate
import org.vitrivr.cottontail.database.queries.components.ComparisonOperator
import org.vitrivr.cottontail.database.queries.components.Predicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.model.basics.CloseableIterator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.exceptions.ValidationException
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Path

/**
 * Represents an index in the Cottontail DB data model. An [Index] belongs to an [Entity] and can be used to index one to many
 * [Column]s. Usually, [Index]es allow for faster data access. They process [Predicate]s and return [Recordset]s.
 *
 * @see Column
 * @see Entity.Tx
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 1.2.2
 */
class NonUniqueHashIndex(override val name: Name.IndexName, override val catalogue: Catalogue, override val columns: Array<ColumnDef<*>>) : Index() {
    /**
     * Index-wide constants.
     */
    companion object {
        const val MAP_FIELD_NAME = "nu_map"
        private val LOGGER = LoggerFactory.getLogger(NonUniqueHashIndex::class.java)
    }

    /** Path to the [NonUniqueHashIndex] file. */
    override val path: Path = this.catalogue.indexForName(this.name).path

    /** The type of [Index] */
    override val type: IndexType = IndexType.HASH_UQ

    /** True since [NonUniqueHashIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** The [NonUniqueHashIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = this.columns

    /** The internal [DB] reference. */
    private val db: DB = this.catalogue.config.mapdb.db(this.path)

    /** Map structure used for [NonUniqueHashIndex]. */
    private val map: HTreeMap<out Value, LongArray> = this.db.hashMap(MAP_FIELD_NAME, this.columns.first().type.serializer(this.columns.size), Serializer.LONG_ARRAY).counterEnable().createOrOpen()

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
    override fun canProcess(predicate: Predicate): Boolean = if (predicate is AtomicBooleanPredicate<*>) {
        predicate.columns.first() == this.columns[0] && (predicate.operator == ComparisonOperator.IN || predicate.operator == ComparisonOperator.EQUAL)
    } else {
        false
    }

    /**
     * Calculates the cost estimate of this [NonUniqueHashIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost = when {
        predicate !is AtomicBooleanPredicate<*> || predicate.columns.first() != this.columns[0] -> Cost.INVALID
        predicate.operator == ComparisonOperator.EQUAL -> Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS, predicate.columns.map { it.physicalSize }.sum().toFloat())
        predicate.operator == ComparisonOperator.IN -> Cost(Cost.COST_DISK_ACCESS_READ * predicate.values.size, Cost.COST_MEMORY_ACCESS * predicate.values.size, predicate.columns.map { it.physicalSize }.sum().toFloat())
        else -> Cost.INVALID
    }

    /**
     * Opens and returns a new [IndexTransaction] object that can be used to interact with this [Index].
     *
     * @param parent If the [Entity.Tx] that requested the [IndexTransaction].
     */
    override fun begin(parent: Entity.Tx): IndexTransaction = Tx(parent)

    /**
     * Closes this [NonUniqueHashIndex] and the associated data structures.
     */
    override fun close() = this.globalLock.write {
        if (!this.closed) {
            this.db.close()
            this.closed = true
        }
    }

    /**
     * A [IndexTransaction] that affects this [Index].
     */
    private inner class Tx(parent: Entity.Tx) : Index.Tx(parent) {
        /**
         * (Re-)builds the [NonUniqueHashIndex].
         */
        override fun rebuild() = this.localLock.read {
            /* Check if this [Tx] allows for writing. */
            checkValidForWrite()

            LOGGER.trace("Rebuilding non-unique hash index {}", this@NonUniqueHashIndex.name)

            /* Clear existing map. */
            this@NonUniqueHashIndex.map.clear()

            /* (Re-)create index entries. */
            val localMap = mutableMapOf<Value, MutableList<Long>>()
            this.parent.scan(this.columns).use { s ->
                s.forEach { record ->
                    val value = record[this.columns[0]]
                            ?: throw ValidationException.IndexUpdateException(this.name, "A value cannot be null for instances of non-unique hash-index but tid=$tid is")
                    if (!localMap.containsKey(value)) {
                        localMap[value] = mutableListOf(record.tupleId)
                    } else {
                        localMap[value]!!.add(record.tupleId)
                    }
                }
            }
            val castMap = this@NonUniqueHashIndex.map as HTreeMap<Value, LongArray>
            localMap.forEach { (value, l) -> castMap[value] = l.toLongArray() }

            LOGGER.trace("Rebuilding non-unique hash complete!")
        }

        /**
         * Updates the [NonUniqueHashIndex] with the provided [Record]. This method determines, whether
         * the [Record] affected by the [DataChangeEvent] should be added or updated
         *
         * @param update Collection of [DataChangeEvent]s to process.
         */
        override fun update(update: Collection<DataChangeEvent>) = this.localLock.read {
            /* Check if this [Tx] allows for writing. */
            checkValidForWrite()

            val localMap = this@NonUniqueHashIndex.map as HTreeMap<Value, LongArray>

            /* Inner function for inserting an entry based on a DataChangeEvent. */
            fun atomicInsert(event: DataChangeEvent) {
                val newValue = event.new?.get(this.columns[0])
                        ?: throw ValidationException.IndexUpdateException(this.name, "Values cannot be null for instances of UniqueHashIndex but tid=${event.new?.tupleId} is.")
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
                        ?: throw ValidationException.IndexUpdateException(this.name, "Values cannot be null for instances of UniqueHashIndex but tid=${event.new?.tupleId} is.")
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

            /** Cast [AtomicBooleanPredicate] (if such a cast is possible).  */
            private val predicate = if (predicate !is AtomicBooleanPredicate<*>) {
                throw QueryException.UnsupportedPredicateException("Index '${this@NonUniqueHashIndex.name}' (non-unique hash-index) does not support predicates of type '${predicate::class.simpleName}'.")
            } else {
                predicate
            }

            /* Perform some sanity checks. */
            init {
                checkValidForRead()
            }

            /** Generates a shared lock on the enclosing [Tx]. This lock is kept until the [CloseableIterator] is closed. */
            private val lock = this@Tx.localLock.readLock()

            /** Pre-fetched [Record]s that match the [Predicate]. */
            private val results = this.prepare()

            /** Flag indicating whether this [CloseableIterator] has been closed. */
            @Volatile
            override var isOpen: Boolean = true
                private set
            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean {
                check(this.isOpen) { "Illegal invocation of hasNext(): This CloseableIterator has been closed." }
                return this.results.isNotEmpty()
            }

            /**
             * Returns the next element in the iteration.
             */
            override fun next(): Record {
                check(this.isOpen) { "Illegal invocation of next(): This CloseableIterator has been closed." }
                return this.results.removeFirst()
            }

            /**
             * Closes this [CloseableIterator] and releases all locks and resources associated with it.
             */
            override fun close() {
                if (this.isOpen) {
                    this@Tx.localLock.unlock(this.lock)
                    this.isOpen = false
                }
            }

            /**
             * Prepares the list of matching [Record]s and returns it.
             */
            private fun prepare(): MutableList<Record> {
                val n = when (this.predicate.operator) {
                    ComparisonOperator.EQUAL -> 1
                    ComparisonOperator.IN -> this.predicate.values.size
                    else -> throw QueryException.UnsupportedPredicateException("Instance of unique hash-index does not support ${this.predicate.operator} comparison operators.")
                }
                return if (this.predicate.not) {
                    val blackList = this.predicate.values.take(n).flatMap {
                        this@NonUniqueHashIndex.map[it]?.asList() ?: emptyList()
                    }.toHashSet()
                    this@NonUniqueHashIndex.map.flatMap { entry ->
                        val value = arrayOf(entry.key)
                        entry.value.filter {
                            !blackList.contains(it)
                        }.map {
                            StandaloneRecord(it, this@NonUniqueHashIndex.produces, value)
                        }
                    }.toMutableList()
                } else {
                    this.predicate.values.take(n).flatMap { v ->
                        this@NonUniqueHashIndex.map[v]?.map { tupleId ->
                            StandaloneRecord(tupleId, this@NonUniqueHashIndex.produces, arrayOf(v))
                        } ?: emptyList()
                    }.toMutableList()
                }
            }
        }

        /** Performs the actual COMMIT operation by rolling back the [IndexTransaction]. */
        override fun performCommit() {
            this@NonUniqueHashIndex.db.commit()
        }

        /** Performs the actual ROLLBACK operation by rolling back the [IndexTransaction]. */
        override fun performRollback() {
            this@NonUniqueHashIndex.db.rollback()
        }

        override fun cleanup() {
            /* No Op. */
        }
    }
}
