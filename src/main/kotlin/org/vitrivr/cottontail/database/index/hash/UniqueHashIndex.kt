package org.vitrivr.cottontail.database.index.hash

import org.mapdb.DBMaker
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

import org.vitrivr.cottontail.model.basics.*
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
 * @author Ralph Gasser
 * @version 1.2.3
 */
class UniqueHashIndex(override val name: Name.IndexName, override val catalogue: Catalogue, override val columns: Array<ColumnDef<*>>) : Index() {

    /**
     * Index-wide constants.
     */
    companion object {
        const val MAP_FIELD_NAME = "map"
        private val LOGGER = LoggerFactory.getLogger(UniqueHashIndex::class.java)
    }

    /** Path to the [UniqueHashIndex] file. */
    override val path: Path = this.catalogue.indexForName(this.name).path

    /** The type of [Index] */
    override val type: IndexType = IndexType.HASH_UQ

    /** True since [UniqueHashIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** The [UniqueHashIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = this.columns

    /** The internal database reference. */
    private val db = if (this.catalogue.config.memoryConfig.forceUnmapMappedFiles) {
        DBMaker.fileDB(this.path.toFile()).fileMmapEnable().transactionEnable().make()
    } else {
        DBMaker.fileDB(this.path.toFile()).fileMmapEnable().transactionEnable().make()
    }

    /** Map structure used for [UniqueHashIndex]. */
    private val map: HTreeMap<out Value, TupleId> = this.db.hashMap(MAP_FIELD_NAME, this.columns.first().type.serializer(this.columns.size), Serializer.LONG_PACKED).counterEnable().createOrOpen()

    init {
        /* Initial commit. */
        this.db.commit()
    }

    /**
     * Flag indicating if this [UniqueHashIndex] has been closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [UniqueHashIndex]. [UniqueHashIndex] can be used to process IN and EQUALS
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
     * Calculates the cost estimate of this [UniqueHashIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost = when {
        predicate !is AtomicBooleanPredicate<*> || predicate.columns.first() != this.columns[0] -> Cost.INVALID
        predicate.operator == ComparisonOperator.EQUAL -> Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS_READ, predicate.columns.map { it.physicalSize }.sum().toFloat())
        predicate.operator == ComparisonOperator.IN -> Cost(Cost.COST_DISK_ACCESS_READ * predicate.values.size, Cost.COST_MEMORY_ACCESS_READ * predicate.values.size, predicate.columns.map { it.physicalSize }.sum().toFloat())
        else -> Cost.INVALID
    }

    /**
     * Opens and returns a new [IndexTransaction] object that can be used to interact with this [Index].
     *
     * @param parent If the [Entity.Tx] that requested the [IndexTransaction].
     */
    override fun begin(parent: Entity.Tx): IndexTransaction = Tx(parent)

    /**
     * Closes this [UniqueHashIndex] and the associated data structures.
     */
    override fun close() = this.globalLock.write {
        if (!this.closed) {
            this.db.close()
            this.closed = true
        }
    }

    /**
     * A [IndexTransaction] that affects this [UniqueHashIndex].
     */
    private inner class Tx(parent: Entity.Tx) : Index.Tx(parent) {
        /**
         * (Re-)builds the [UniqueHashIndex].
         **/
        override fun rebuild() = this.localLock.read {
            /* Check if this [Tx] allowed to write. */
            checkValidForWrite()

            LOGGER.trace("Rebuilding unique hash index {}", this@UniqueHashIndex.name)

            /* Clear existing map. */
            this@UniqueHashIndex.map.clear()

            /* (Re-)create index entries. */
            val localMap = this@UniqueHashIndex.map as HTreeMap<Value, TupleId>
            this.parent.scan(this@UniqueHashIndex.columns).use { s ->
                s.forEach { record ->
                    val value = record[this.columns[0]]
                            ?: throw ValidationException.IndexUpdateException(this.name, "A value cannot be null for instances of non-unique hash-index but tid=$tid is")
                    if (!localMap.containsKey(value)) {
                        localMap[value] = record.tupleId
                    } else {
                        LOGGER.warn("Value must be unique for instances of unique hash-index but '$value' (tid=$tid) is not! Skipping entry...")
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
        override fun update(update: Collection<DataChangeEvent>) = this.localLock.read {
            /* Check if this [Tx] allowed to write. */
            checkValidForWrite()
            val localMap = this@UniqueHashIndex.map as HTreeMap<Value, TupleId>

            /* Define action for inserting an entry based on a DataChangeEvent. */
            fun atomicInsert(event: DataChangeEvent) {
                val newValue = event.new?.get(this.columns[0])
                        ?: throw ValidationException.IndexUpdateException(this.name, "Values cannot be null for instances of UniqueHashIndex but tid=${event.new?.tupleId} is.")
                localMap[newValue] = event.new.tupleId
            }

            /* Define action for deleting an entry based on a DataChangeEvent. */
            fun atomicDelete(event: DataChangeEvent) {
                val oldValue = event.old?.get(this.columns[0])
                        ?: throw ValidationException.IndexUpdateException(this.name, "Values cannot be null for instances of UniqueHashIndex but tid=${event.new?.tupleId} is.")
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
        override fun filter(predicate: Predicate): CloseableIterator<Record> = object : CloseableIterator<Record> {

            /** Cast [AtomicBooleanPredicate] (if such a cast is possible).  */
            private val predicate = if (predicate !is AtomicBooleanPredicate<*>) {
                throw QueryException.UnsupportedPredicateException("Index '${this@UniqueHashIndex.name}' (non-unique hash-index) does not support predicates of type '${predicate::class.simpleName}'.")
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

            /** Flag indicating whether this [CloseableIterator] is open and ready for use. */
            @Volatile
            override var isOpen = true
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
                    val blackList = this.predicate.values.take(n).mapNotNull { this@UniqueHashIndex.map[it] }.toHashSet()
                    this@UniqueHashIndex.map.map {
                        StandaloneRecord(it.value, this@UniqueHashIndex.produces, arrayOf(it.key))
                    }.filter {
                        !blackList.contains(it.tupleId)
                    }.toMutableList()
                } else {
                    this.predicate.values.take(n).mapNotNull {
                        val tupleId = this@UniqueHashIndex.map[it]
                        if (tupleId != null) {
                            StandaloneRecord(tupleId, this@UniqueHashIndex.produces, arrayOf(it))
                        } else {
                            null
                        }
                    }.toMutableList()
                }
            }
        }

        /** Performs the actual COMMIT operation by rolling back the [IndexTransaction]. */
        override fun performCommit() {
            this@UniqueHashIndex.db.commit()
        }

        /** Performs the actual ROLLBACK operation by rolling back the [IndexTransaction]. */
        override fun performRollback() {
            this@UniqueHashIndex.db.rollback()
        }

        override fun cleanup() {
            /* No Op. */
        }
    }
}
