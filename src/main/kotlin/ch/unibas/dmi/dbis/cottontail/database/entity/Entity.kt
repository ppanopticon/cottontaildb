package ch.unibas.dmi.dbis.cottontail.database.entity

import ch.unibas.dmi.dbis.cottontail.database.column.Column
import ch.unibas.dmi.dbis.cottontail.database.general.DBO
import ch.unibas.dmi.dbis.cottontail.database.general.TransactionStatus
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnTransaction
import ch.unibas.dmi.dbis.cottontail.database.column.mapdb.MapDBColumn
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.index.IndexTransaction
import ch.unibas.dmi.dbis.cottontail.database.index.IndexType
import ch.unibas.dmi.dbis.cottontail.database.queries.AtomicBooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.ComparisonOperator
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.model.basics.*
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.recordset.StandaloneRecord

import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.exceptions.TransactionException
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.utilities.name.*
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write

import org.mapdb.*

import java.lang.IllegalArgumentException
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock

import java.util.concurrent.locks.StampedLock
import java.util.stream.Collectors
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Represents a single entity in the Cottontail DB data model. An [Entity] has name that must remain unique within a [Schema].
 * The [Entity] contains one to many [Column]s holding the actual data. Hence, it can be seen as a table containing tuples.
 *
 * Calling the default constructor for [Entity] opens that [Entity]. It can only be opened once due to file locks and it
 * will remain open until the [Entity.close()] method is called.
 *
 * @see Schema
 * @see Column
 * @see Entity.Tx
 *
 * @author Ralph Gasser
 * @version 1.4
 */
class Entity(override val name: Name, override val parent: Schema) : DBO {
    /** Constant FQN of the [Entity] object. */
    override val fqn: Name = this.parent.fqn.append(this.name)

    /** The [Path] to the [Entity]'s main folder. */
    override val path: Path = this.parent.path.resolve("entity_$name")

    /** Internal reference to the [StoreWAL] underpinning this [Entity]. */
    private val store: CottontailStoreWAL = try {
        CottontailStoreWAL.make(file = this.path.resolve(FILE_CATALOGUE).toString(), volumeFactory = this.parent.parent.config.volumeFactory, fileLockWait = this.parent.parent.config.lockTimeout)
    } catch (e: DBException) {
        throw DatabaseException("Failed to open entity '$fqn': ${e.message}'.")
    }

    /** The header of this [Entity]. */
    private val header: EntityHeader
        get() = this.store.get(HEADER_RECORD_ID, EntityHeaderSerializer) ?: throw DatabaseException.DataCorruptionException("Failed to open header of entity '$fqn'!")

    /** An internal lock that is used to synchronize concurrent read & write access to this [Entity] by different [Entity.Tx]. */
    private val txLock = StampedLock()

    /** An internal lock that is used to synchronize structural changes to an [Entity] (e.g. closing or deleting) with running [Entity.Tx]. */
    private val globalLock = StampedLock()

    /** List of all the [Column]s associated with this [Entity]. */
    private val columns: Collection<Column<*>> = this.header.columns.map {
        MapDBColumn<Value>(Name(this.store.get(it, Serializer.STRING)
                ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$fqn': Could not read column definition at position $it!")), this)
    }

    /** List of all the [Index]es associated with this [Entity]. */
    private val indexes: MutableCollection<Index> = this.header.indexes.map {idx ->
        val index = this.store.get(idx, IndexEntrySerializer) ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$fqn': Could not read index definition at position $idx!")
        index.type.open(Name(index.name), this, index.columns.map { col -> this.columnForName(Name(col)) ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$fqn': It hosts an index for column '$col' that does not exist on the entity!") }.toTypedArray())
    }.toMutableSet()

    /**
     * Status indicating whether this [Entity] is open or closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    /**
     * Creates and returns an [EntityStatistics] snapshot.
     *
     * @return [EntityStatistics] for this [Entity].
     */
    val statistics: EntityStatistics
        get() = this.header.let { EntityStatistics(it.columns.size, it.size, this.columns.first().maxTupleId) }

    /**
     * Checks if this [Entity] can process the provided [Predicate] natively (without index).
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    fun canProcess(predicate: Predicate): Boolean = when {
        predicate is BooleanPredicate && predicate.atomics.all { it.operator != ComparisonOperator.LIKE } -> true
        else -> false
    }

    /**
     * Returns all [ColumnDef] for the [Column]s contained in this [Entity].
     *
     * @return Collection of [ColumnDef].
     */
    fun allColumns(): Collection<ColumnDef<*>> = this.columns.map { it.columnDef }

    /**
     * Returns all [Index]es for this [Entity].
     *
     * @return Collection of [Index].
     */
    fun allIndexes(): Collection<Index> = this.indexes

    /**
     * Returns the [ColumnDef] for the specified [Name]. The name can be either a [NameType.SIMPLE] or [NameType.FQN]. In the
     * first case, it will be used to construct a FQN based on the FQN of this [Entity].
     *
     * @param name The [Name] of the [Column].
     * @return [ColumnDef] of the [Column].
     */
    fun columnForName(name: Name): ColumnDef<*>? = this.columns.find {
        if (name.type == NameType.FQN) {
            it.name == name.normalize(this.fqn)
        } else {
            it.name == name
        }
    }?.columnDef

    /**
     * Checks, if this [Entity] has an index for the given [ColumnDef] and (optionally) of the given [IndexType]
     *
     * @param column The [ColumnDef] for which to check.
     * @param type The [IndexType] for which to check.
     * @return True if this [Entity] has an [Index] that satisfies the condition, false otherwise.
     */
    fun hasIndexForColumn(column: ColumnDef<*>, type: IndexType? = null): Boolean = this.indexes.find { it.columns.contains(column) && (type == null || it.type == type) } != null

    /**
     * Creates the [Index] with the given settings
     *
     * @param name [Name] of the [Index] to create.
     * @param type Type of the [Index] to create.
     * @param columns The list of [columns] to [Index].
     */
    fun createIndex(name: Name, type: IndexType, columns: Array<ColumnDef<*>>, params: Map<String,String> = emptyMap()) {
        /* Check the type of name. */
        if (name.type != NameType.SIMPLE) {
            throw IllegalArgumentException("The provided name '$name' is of type '${name.type}  and cannot be used to access an index through an entity.")
        }

        /* Creates new index. */
        val index: Index = this.globalLock.write {
            val indexEntry = this.header.indexes.map {
                Pair(it, this.store.get(it, IndexEntrySerializer) ?: throw DatabaseException.DataCorruptionException("Failed to create index '${this.fqn.append(name)}': Could not read index definition at position $it!"))
            }.find { Name(it.second.name) == name }

            if (indexEntry != null) throw DatabaseException.IndexAlreadyExistsException(this.fqn.append(name))

            /* Creates and opens the index. */
            val newIndex = type.create(name, this, columns, params)
            this.indexes.add(newIndex)

            /* Update catalogue + header. */
            try {
                /* Update catalogue. */
                val sid = this.store.put(IndexEntry(name.name, type, false, columns.map { it.name.name }.toTypedArray()), IndexEntrySerializer)

                /* Update header. */
                val new = this.header.let { EntityHeader(it.size, it.created, System.currentTimeMillis(), it.columns, it.indexes.copyOf(it.indexes.size + 1)) }
                new.indexes[new.indexes.size-1] = sid
                this.store.update(Entity.HEADER_RECORD_ID, new, EntityHeaderSerializer)
                this.store.commit()
            } catch (e: DBException) {
                this.store.rollback()
                val pathsToDelete = Files.walk(newIndex.path).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
                pathsToDelete.forEach { Files.delete(it) }
                throw DatabaseException("Failed to create index '${this.fqn.append(name)}' due to a storage exception: ${e.message}")
            }

            newIndex
        }

        /* Rebuilds the index. */
        try {
            val tx = Tx(readonly = false)
            tx.index(name)?.rebuild()
            tx.close()
        } catch (e: Throwable) {
            val pathsToDelete = Files.walk(index.path).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
            pathsToDelete.forEach { Files.delete(it) }
            throw DatabaseException("Failed to create index '${this.fqn.append(name)}' due to a build failure: ${e.message}")
        }
    }



    /**
     * Drops the [Index] with the given name.
     *
     * @param name [Name] of the [Index] to drop.
     */
    fun dropIndex(name: Name) = this.globalLock.write {
        /* Check the type of name. */
        if (name.type != NameType.SIMPLE) {
            throw IllegalArgumentException("The provided name '$name' is of type '${name.type}  and cannot be used to access an index through an entity.")
        }

        val indexEntry = this.header.indexes.map {
            Pair(it, this.store.get(it, IndexEntrySerializer) ?: throw DatabaseException.DataCorruptionException("Failed to drop index '$fqn.$name': Could not read index definition at position $it!"))
        }.find { Name(it.second.name) == name }?.let { ie ->
            Triple(ie.first, ie.second, this.indexes.find { it.name == Name(ie.second.name) })
        } ?: throw DatabaseException.IndexDoesNotExistException(this.fqn.append(name))

        /* Close index. */
        indexEntry.third!!.close()
        this.indexes.remove(indexEntry.third!!)

        /* Update header. */
        try {
            val new = this.header.let { EntityHeader(it.size, it.created, System.currentTimeMillis(), it.columns, it.indexes.filter { it != indexEntry.first }.toLongArray()) }
            this.store.update(HEADER_RECORD_ID, new, EntityHeaderSerializer)
            this.store.commit()
        } catch (e: DBException) {
            this.store.rollback()
            throw DatabaseException("Failed to drop index '$fqn.$name' due to a storage exception: ${e.message}")
        }

        /* Delete files that belong to the index. */
        if (indexEntry.third != null) {
            val pathsToDelete = Files.walk(indexEntry.third!!.path).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
            pathsToDelete.forEach { Files.delete(it) }
        }
    }

    /**
     * Updates the [Index] with the given name.
     *
     * @param name The name of the [Index]
     */
    fun updateIndex(name: Name) = Tx(readonly = false).begin {tx ->
        val itx = tx.index(name)
        if (itx != null) {
            itx.rebuild()
        } else {
            throw DatabaseException.IndexDoesNotExistException(this.fqn.append(name))
        }
        true
    }

    /**
     * Updates all [Index]es for this [Entity].
     */
    fun updateAllIndexes() = Tx(readonly = false).begin {tx ->
        tx.indexes().forEach { itx ->
            itx.rebuild()
        }
        true
    }

    /**
     * Closes the [Entity]. Closing an [Entity] is a delicate matter since ongoing [Entity.Tx] objects as well as all involved [Column]s are involved.
     * Therefore, access to the method is mediated by an global [Entity] wide lock.
     */
    override fun close() = this.globalLock.write {
        this.columns.forEach { it.close() }
        this.store.close()
        this.closed = true
    }

    /**
     * Handles finalization, in case the Garbage Collector reaps a cached [Entity] soft-reference.
     */
    @Synchronized
    protected fun finalize() {
        if (!this.closed) {
            this.close()
        }
    }

    /**
     * Companion object of the [Entity]
     */
    companion object {
        /** Filename for the [Entity] catalogue.  */
        const val FILE_CATALOGUE = "index.db"

        /** Filename for the [Entity] catalogue.  */
        const val HEADER_RECORD_ID = 1L
    }

    /**
     * A [Tx] that affects this [Entity].
     *
     * Opening such a [Tx] will spawn a associated [Column.Tx] for every [Column] associated with this [Entity].
     */
    inner class Tx(override val readonly: Boolean, override val tid: UUID = UUID.randomUUID(), columns: Array<ColumnDef<*>>? = null, ommitIndex: Boolean = false) : EntityTransaction {

        /** List of [ColumnTransaction]s associated with this [Entity.Tx]. */
        private val colTxs: List<ColumnTransaction<*>> = if (columns != null && this.readonly) {
            columns.map { it1 ->  this@Entity.columns.firstOrNull { it2 -> it1.isEquivalent(it2.columnDef) }?.newTransaction(this.readonly, this.tid) ?: throw QueryException.ColumnDoesNotExistException(it1)}
        } else {
            this@Entity.columns.map { it.newTransaction(this.readonly, tid) }
        }

        /** List of [IndexTransaction] associated with this [Entity.Tx]. */
        private val indexTxs: Collection<IndexTransaction> = if (!ommitIndex) {
            this@Entity.indexes.map { it.Tx(this.readonly, this) }
        } else {
            emptyList()
        }

        /** List of all [ColumnDef]s affected by this [Entity.Tx]. */
        private val columns = this.colTxs.map { it.columnDef }.toTypedArray()

        /** Flag indicating whether or not this [Entity.Tx] was closed */
        @Volatile
        override var status: TransactionStatus = TransactionStatus.CLEAN
            private set

        /** Tries to acquire a global read-lock on this [Entity]. */
        init {
            if (this@Entity.closed) {
                throw TransactionException.TransactionDBOClosedException(tid)
            }
        }

        /** Obtains a global (non-exclusive) read-lock on [Entity]. Prevents enclosing [Entity] from being closed while this [Entity.Tx] is still in use. */
        private val globalStamp = this@Entity.globalLock.readLock()

        /** Obtains transaction lock on [Entity]. Prevents concurrent read & write access to the enclosing [Entity]. */
        private val txStamp = if (this.readonly) {
            this@Entity.txLock.readLock()
        } else {
            this@Entity.txLock.writeLock()
        }

        /** A [ReentrantReadWriteLock] local to this [Entity.Tx]. It makes sure, that this [Entity] cannot be committed, closed or rolled back while it is being used. */
        private val localLock = ReentrantReadWriteLock()

        /**
         * Commits all changes made through this [Entity.Tx] since the last commit or rollback.
         */
        @Synchronized
        override fun commit() = this.localLock.write {
            if (this.status == TransactionStatus.DIRTY) {
                this.colTxs.forEach { it.commit() }
                this@Entity.store.commit()
                this.status = TransactionStatus.CLEAN
            }
        }

        /**
         * Rolls all changes made through this [Entity.Tx] back to the last commit.
         */
        @Synchronized
        override fun rollback() = this.localLock.write {
            if (this.status == TransactionStatus.DIRTY) {
                this.colTxs.forEach { it.rollback() }
                this@Entity.store.rollback()
                this.status = TransactionStatus.CLEAN
            }
        }

        /**
         * Closes this [Entity.Tx] and thereby releases all the [Column.Tx] and the global lock. Closed [Entity.Tx] cannot be used anymore!
         */
        @Synchronized
        override fun close() = this.localLock.write {
            if (this.status != TransactionStatus.CLOSED) {
                if (this.status == TransactionStatus.DIRTY) {
                    this.rollback()
                }
                this.indexTxs.forEach { it.close() }
                this.colTxs.forEach { it.close() }
                this.status = TransactionStatus.CLOSED
                this@Entity.txLock.unlock(this.txStamp)
                this@Entity.globalLock.unlockRead(this.globalStamp)
            }
        }

        /**
         * Reads the values of one or many [Column]s and returns it as a [Tuple]
         *
         * @param tupleId The ID of the desired entry.
         * @return The desired [Tuple].
         *
         * @throws DatabaseException If tuple with the desired ID doesn't exist OR is invalid.
         */
        fun read(tupleId: Long): Record = this.localLock.read {
            checkValidForRead()
            checkValidTupleId(tupleId)

            /* Return value of all the desired columns. */
            return StandaloneRecord(tupleId, this.columns).assign(this.colTxs.map { it.read(tupleId) }.toTypedArray() )
        }

        /**
         * Reads the specified values of one or many [Column]s and returns them as a [Recordset]
         *
         * @param tupleId The ID of the desired entry.
         * @return The resulting [Recordset].
         *
         * @throws DatabaseException If tuple with the desired ID doesn't exist OR is invalid.
         */
        fun readMany(tupleIds: Collection<Long>): Recordset = this.localLock.read {
            checkValidForRead()
            val dataset = Recordset(this.columns)
            tupleIds.forEach { tid ->
                checkValidTupleId(tid)
                dataset.addRowUnsafe(tid, this.colTxs.map { it.read(tid) }.toTypedArray())
            }
            return dataset
        }

        /**
         * Reads all values of one or many [Column]s and returns them as a [Recordset].
         *
         * @return The resulting [Recordset].
         */
        fun readAll(): Recordset = this.localLock.read {
            checkValidForRead()

            val dataset = Recordset(this.columns)
            val data = Array<Value?>(this.columns.size) { null }

            this.colTxs[0].forEach {
                data[0] = it.values[0]
                for (i in 1 until columns.size) {
                    data[i] = this.colTxs[i].read(it.tupleId)
                }
                dataset.addRowUnsafe(it.tupleId, data)
            }
            return dataset
        }

        /**
         * Returns the number of entries in this [Entity].
         *
         * @return The number of entries in this [Entity].
         */
        override fun count(): Long = this.localLock.read {
            checkValidForRead()
            return this@Entity.header.size
        }

        /**
         * Applies the provided function to each entry found in this [Entity]. The provided function cannot not change
         * the data stored in the [Entity]!
         *
         * @param action The function to apply to each [Entity] entry.
         */
        override fun forEach(action: (Record) -> Unit) = forEach(1L, this@Entity.statistics.maxTupleId, action)

        /**
         * Applies the provided function to each entry found in the given range in this [Entity]. The provided function
         * cannot not change the data stored in the [Entity]!
         *
         * @param from The tuple ID of the first [Record] to iterate over.
         * @param to The tuple ID of the last [Record] to iterate over.
         * @param action The function to apply to each [Entity] entry.
         */
        override fun forEach(from: Long, to: Long, action: (Record) -> Unit) = this.localLock.read {
            checkValidForRead()
            val data = Array<Value?>(columns.size) { null }
            this.colTxs[0].forEach(from, to) {
                data[0] = it.values[0]
                for (i in 1 until columns.size) {
                    data[i] = this.colTxs[i].read(it.tupleId)
                }
                action(StandaloneRecord(tupleId = it.tupleId, columns = columns, init = data))
            }
        }


        /**
         * Applies the provided mapping function on each [Record] found in the given range in this [Entity], returning a collection of the desired output values.
         *
         * @param action The mapping that should be applied to each [Tuple].
         *
         * @return A collection of Pairs mapping the tupleId to the generated value.
         */
        override fun <R> map(action: (Record) -> R): Collection<R> = map(1L, this@Entity.statistics.maxTupleId, action)

        /**
         * Applies the provided mapping function on each [Record] found in this [Entity], returning a collection of the desired output values.
         *
         * @param from The tuple ID of the first [Record] to iterate over.
         * @param to The tuple ID of the last [Record] to iterate over.
         * @param action The mapping that should be applied to each [Tuple].
         *
         * @return A collection of Pairs mapping the tupleId to the generated value.
         */
        override fun <R> map(from: Long, to: Long, action: (Record) -> R): Collection<R> = this.localLock.read {
            checkValidForRead()

            val data = Array<Value?>(columns.size) { null }
            val list = mutableListOf<R>()

            this.colTxs[0].forEach(from, to) {
                data[0] = it.values[0]
                for (i in 1 until columns.size) {
                    data[i] = this.colTxs[i].read(it.tupleId)
                }
                list.add(action(StandaloneRecord(it.tupleId, columns).assign(data)))
            }
            return list
        }

        /**
         * Checks if this [Entity.Tx] can process the provided [Predicate] natively (without index).
         *
         * @param predicate [Predicate] to check.
         * @return True if [Predicate] can be processed, false otherwise.
         */
        override fun canProcess(predicate: Predicate): Boolean = predicate is BooleanPredicate

        /**
         * Reads all values of one or many [Column]s and returns those that match the provided predicate as a [Recordset]. Explicitly scans the entire [Entity] and
         * does not use any [Index] structures.
         *
         * @param predicate The [Predicate] to apply. Only columns contained in that [Predicate] will be read.
         * @return The resulting [Recordset].
         */
        override fun filter(predicate: Predicate): Recordset = this.localLock.read {
            checkValidForRead()
            checkColumnsExist(*predicate.columns.toTypedArray())

            val dataset = Recordset(this.columns)
            val data = Array<Value?>(this.columns.size) { null }

            /* Handle filter() for different cases. */
            when (predicate) {
                /* Case 1: Predicate affects single column (AtomicBooleanPredicate). */
                is AtomicBooleanPredicate<*> -> {
                    this.colTxs.first { it.columnDef.isEquivalent(predicate.columns.first()) }.forEach(predicate) {
                        for (i in columns.indices) {
                            data[i] = this.colTxs[i].read(it.tupleId)
                        }
                        dataset.addRowUnsafe(it.tupleId, data)
                    }
                }
                /* Case 2 (general): Multi-column boolean predicate. */
                is BooleanPredicate -> {
                    this.colTxs[0].forEach {
                        data[0] = it.values[0]
                        for (i in 1 until columns.size) {
                            data[i] = this.colTxs[i].read(it.tupleId)
                        }
                        dataset.addRowIfUnsafe(it.tupleId , predicate, data)
                    }
                }
                else -> throw QueryException.UnsupportedPredicateException("Entity#filter() does not support predicates of type '${predicate::class.simpleName}'.")
            }
            return dataset
        }

        /**
         * Applies the provided action to each [Record] that matches the given [Predicate]. Explicitly scans the entire [Entity] and
         * does not use any [Index] structures.
         *
         * @param predicate The [BooleanPredicate] to filter [Record]s.
         * @param action The action that should be applied.
         */
        override fun forEach(predicate: Predicate, action: (Record) -> Unit) = forEach(1L, this@Entity.statistics.maxTupleId, predicate, action)

        /**
         * Applies the provided action to each [Record] in the given range that matches the given [Predicate]. Explicitly scans the entire [Entity] and
         * does not use any [Index] structures.
         *
         * @param from The tuple ID of the first [Record] to iterate over.
         * @param to The tuple ID of the last [Record] to iterate over.
         * @param predicate The [Predicate] to filter [Record]s.
         * @param action The action that should be applied.
         */
        override fun forEach(from: Long, to: Long, predicate: Predicate, action: (Record) -> Unit) = this.localLock.read {
            checkValidForRead()
            checkColumnsExist(*predicate.columns.toTypedArray())

            /* Extract necessary data structures. */
            val data = Array<Value?>(this.columns.size) { null }

            /* Handle forEach() for different cases. */
            when (predicate) {
                /* Case 1: Predicate affects single column (AtomicBooleanPredicate). */
                is AtomicBooleanPredicate<*> -> this.colTxs.first { it.columnDef.isEquivalent(predicate.columns.first()) }.forEach(from, to, predicate) {
                    for (i in columns.indices) {
                        data[i] = this.colTxs[i].read(it.tupleId)
                    }
                    action(StandaloneRecord(it.tupleId, columns).assign(data))
                }
                /* Case 2 (general): Multi-column boolean predicate. */
                is BooleanPredicate -> this.colTxs[0].forEach(from, to){
                    data[0] = it.values[0]
                    for (i in 1 until columns.size) {
                        data[i] = this.colTxs[i].read(it.tupleId)
                    }
                    val record = StandaloneRecord(it.tupleId, columns).assign(data)
                    if (predicate.matches(record)) {
                        action(record)
                    }
                }
                else -> throw QueryException.UnsupportedPredicateException("Entity#forEach() does not support predicates of type '${predicate::class.simpleName}'.")
            }
        }

        /**
         * Applies the provided mapping function to each [Record] that matches the given [Predicate]. Explicitly scans the entire [Entity] and
         * does not use any [Index] structures.
         *
         * @param predicate The [Predicate] to filter [Record]s.
         * @param action The mapping function that should be applied.
         * @return Collection of the results of the mapping function.
         */
        override fun <R> map(predicate: Predicate, action: (Record) -> R): Collection<R> = map(1L, this@Entity.statistics.maxTupleId, predicate, action)

        /**
         * Applies the provided mapping function to each [Record] in the given range that matches the given [Predicate]. Explicitly scans the entire [Entity] and
         * does not use any [Index] structures.
         *
         * @param from The tuple ID of the first [Record] to iterate over.
         * @param to The tuple ID of the last [Record] to iterate over.
         * @param predicate The [Predicate] to filter [Record]s.
         * @param action The mapping function that should be applied.
         * @return Collection of the results of the mapping function.
         */
        override fun <R> map(from: Long, to: Long, predicate: Predicate, action: (Record) -> R): Collection<R> = this.localLock.read {
            checkValidForRead()
            checkColumnsExist(*predicate.columns.toTypedArray())

            val data = Array<Value?>(columns.size) { null }
            val list = mutableListOf<R>()

            /* Handle map() for different cases. */
            when (predicate) {
                /* Case 1: Predicate affects single column (AtomicBooleanPredicate). */
                is AtomicBooleanPredicate<*> -> this.colTxs.first { it.columnDef.isEquivalent(predicate.columns.first()) }.forEach(from, to, predicate) {
                    for (i in columns.indices) {
                        data[i] = this.colTxs[i].read(it.tupleId)
                    }
                    list.add(action(StandaloneRecord(it.tupleId, columns).assign(data)))
                }
                /* Case 2 (general): Multi-column boolean predicate. */
                is BooleanPredicate -> this.colTxs[0].forEach(from, to) {
                    data[0] = it.values[0]
                    for (i in 1 until columns.size) {
                        data[i] = this.colTxs[i].read(it.tupleId)
                    }
                    val record = StandaloneRecord(it.tupleId, columns).assign(data)
                    if (predicate.matches(record)) {
                        list.add(action(record))
                    }
                } else -> throw QueryException.UnsupportedPredicateException("Entity#forEach() does not support predicates of type '${predicate::class.simpleName}'.")
            }
            return list
        }

        /**
         * Returns a collection of all the [IndexTransaction] available to this [EntityTransaction],.
         *
         * @return Collection of [IndexTransaction]s. May be empty.
         */
        override fun indexes(): Collection<IndexTransaction> = this.localLock.read {
            this.indexTxs
        }

        /**
         * Returns a collection of all the [IndexTransaction] available to this [EntityTransaction], that match the given [ColumnDef] and [IndexType] constraint.
         *
         * @param columns The list of [ColumnDef] that should be handled by this [IndexTransaction].
         * @param type The (optional) [IndexType]. If omitted, [IndexTransaction]s of any type are returned.
         *
         * @return Collection of [IndexTransaction]s. May be empty.
         */
        override fun indexes(columns: Array<ColumnDef<*>>?, type: IndexType?): Collection<IndexTransaction> = this.localLock.read {
            this.indexTxs.filter { tx ->
                (columns?.all { tx.columns.contains(it) } ?: true) && (type == null || tx.type == type)
            }
        }

        /**
         * Returns the [IndexTransaction] for the given [Name] or null, if such a [IndexTransaction] doesn't exist.
         *
         * @param name The [Name] of the [Index] the [IndexTransaction] belongs to.
         * @return Optional [IndexTransaction]
         */
        override fun index(name: Name): IndexTransaction? = this.localLock.read {
            this.indexTxs.find { name.match(it.name) == Match.EQUAL || name.match(it.name) == Match.EQUIVALENT }
        }

        /**
         * Attempts to insert the provided [Tuple] into the [Entity]. Columns specified in the [Tuple] that are not part
         * of the [Entity] will cause an error!
         *
         * @param record The [Record] that should be inserted.
         * @return The ID of the record or null, if nothing was inserted.
         * @throws TransactionException If some of the sub-transactions on [Column] level caused an error.
         * @throws DatabaseException If a general database error occurs during the insert.
         */
        fun insert(record: Record): Long? = this.localLock.read {
            checkValidForWrite()
            checkColumnsExist(*record.columns) /* Perform sanity check on columns before locking. */

            try {
                var lastRecId: Long? = null
                for (i in this.colTxs.indices) {
                    val recId = (this.colTxs[i] as ColumnTransaction<Value>).insert(record[this.columns[i]])
                    if (lastRecId != recId && lastRecId != null) {
                        throw DatabaseException.DataCorruptionException("Entity ${this@Entity.fqn} is corrupt. Insert did not yield same record ID for all columns involved!")
                    }
                    lastRecId = recId
                }

                /* Update the header of this entity. */
                if (lastRecId != null) {
                    val header = this@Entity.header
                    header.size += 1
                    header.modified = System.currentTimeMillis()
                    this@Entity.store.update(HEADER_RECORD_ID, header, EntityHeaderSerializer)
                }

                return lastRecId
            } catch (e: DatabaseException) {
                this.status = TransactionStatus.ERROR
                throw e
            } catch (e: DBException) {
                this.status = TransactionStatus.ERROR
                throw DatabaseException("Inserting record failed due to an error in the underlying storage: ${e.message}.")
            }
        }

        /**
         * Attempts to insert the provided [Tuple]s into the [Entity]. Columns specified in the [Tuple] that are not part
         * of the [Entity] will cause an error!
         *
         * @param tuples The [Tuple] that should be inserted.
         * @return The ID of the record or null, if nothing was inserted.
         * @throws TransactionException If some of the sub-transactions on [Column] level caused an error.
         * @throws DatabaseException If a general database error occurs during the insert.
         */
        fun insertAll(tuples: Collection<Record>): Collection<Long?> = this.localLock.read {
            checkValidForWrite()
            tuples.forEach { checkColumnsExist(*it.columns) }

            try {
                /* Perform delete on each column. */
                val tuplesIds = tuples.map { record ->
                    var lastRecId: Long? = null
                    for (i in this.colTxs.indices) {
                        val recId = (this.colTxs[i] as ColumnTransaction<Value>).insert(record[this.columns[i]])
                        if (lastRecId != recId && lastRecId != null) {
                            throw DatabaseException.DataCorruptionException("Entity ${this@Entity.fqn} is corrupt. Insert did not yield same record ID for all columns involved!")
                        }
                        lastRecId = recId
                    }
                    lastRecId
                }

                /* Update header. */
                val header = this@Entity.header
                header.size += tuples.size
                header.modified = System.currentTimeMillis()
                this@Entity.store.update(HEADER_RECORD_ID, header, EntityHeaderSerializer)

                return tuplesIds
            } catch (e: DatabaseException) {
                this.status = TransactionStatus.ERROR
                throw e
            } catch (e: DBException) {
                this.status = TransactionStatus.ERROR
                throw DatabaseException("Inserting records failed due to an error in the underlying storage: ${e.message}.")
            }
        }

        /**
         * Attempts to delete the provided [Tuple] from the [Entity]. This tasks will set this [Entity.Tx] to
         * [TransactionStatus.DIRTY] and acquire a [Entity]-wide write lock until the [Entity.Tx] either commit
         * or rollback is issued.
         *
         * @param tupleId The ID of the [Tuple] that should be deleted.
         *
         * @throws TransactionException If some of the sub-transactions on [Column] level caused an error.
         * @throws DatabaseException If a general database error occurs during the insert.
         */
        override fun delete(tupleId: Long) = this.localLock.read {
            checkValidForWrite()
            try {
                /* Perform delete on each column. */
                this.colTxs.forEach { it.delete(tupleId) }

                /* Update header. */
                val header = this@Entity.header
                header.size -= 1
                header.modified = System.currentTimeMillis()
                this@Entity.store.update(HEADER_RECORD_ID, header, EntityHeaderSerializer)
            } catch (e: DBException) {
                this.status = TransactionStatus.ERROR
                throw DatabaseException("Deleting record $tid failed due to an error in the underlying storage: ${e.message}.")
            }
        }

        /**
         * Attempts to delete all the provided [Tuple] from the [Entity]. This tasks will set this [Entity.Tx] to
         * [TransactionStatus.DIRTY] and acquire a [Entity]-wide write lock until the [Entity.Tx] either commit
         * or rollback is issued.
         *
         * @param tupleIds The IDs of the [Tuple]s that should be deleted.
         *
         * @throws TransactionException If some of the sub-transactions on [Column] level caused an error.
         * @throws DatabaseException If a general database error occurs during the insert.
         */
        override  fun deleteAll(tupleIds: Collection<Long>) = this.localLock.read {
            checkValidForWrite()
            try {
                /* Perform delete on each column. */
                tupleIds.forEach { tupleId ->
                    this.colTxs.forEach { it.delete(tupleId) }
                }

                /* Update header. */
                val header = this@Entity.header
                header.size -= tupleIds.size
                header.modified = System.currentTimeMillis()
                this@Entity.store.update(HEADER_RECORD_ID, header, EntityHeaderSerializer)
            } catch (e: DBException) {
                this.status = TransactionStatus.ERROR
                throw DatabaseException("Deleting records failed due to an error in the underlying storage: ${e.message}.")
            }
        }

        /**
         * Check if all the provided [Column]s exist on this [Entity] and that they have the type that was expected!
         *
         * @params The list of [Column]s that should be checked.
         */
        private fun checkColumnsExist(vararg columns: ColumnDef<*>) = columns.forEach { it1 ->
            if (!this.columns.any { it2 -> it1.isEquivalent(it2) }) {
                throw TransactionException.ColumnUnknownException(this.tid, it1)
            }
        }

        /**
         * Checks if the provided tupleID is valid. Otherwise, an exception will be thrown.
         *
         * @param tupleId The tuple ID to check.
         */
        private fun checkValidTupleId(tupleId: Long) {
            if (tupleId < HEADER_RECORD_ID) {
                throw TransactionException.InvalidTupleId(tid, tupleId)
            }
        }

        /**
         * Checks if this [Entity.Tx] is in a valid state for read operations to happen.
         */
        @Synchronized
        private fun checkValidForRead() {
            if (this.status == TransactionStatus.CLOSED) throw TransactionException.TransactionClosedException(tid)
            if (this.status == TransactionStatus.ERROR) throw TransactionException.TransactionInErrorException(tid)
        }

        /**
         * Checks if this [Entity.Tx] is in a valid state for write operations to happen.
         */
        @Synchronized
        private fun checkValidForWrite() {
            if (this.readonly) throw TransactionException.TransactionReadOnlyException(tid)
            if (this.status == TransactionStatus.CLOSED) throw TransactionException.TransactionClosedException(tid)
            if (this.status == TransactionStatus.ERROR) throw TransactionException.TransactionInErrorException(tid)
            if (this.status != TransactionStatus.DIRTY) {
                this.status = TransactionStatus.DIRTY
            }
        }
    }
}
