package org.vitrivr.cottontail.database.entity

import org.mapdb.CottontailStoreWAL
import org.mapdb.DBException
import org.mapdb.Serializer
import org.mapdb.StoreWAL
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.column.ColumnTransaction
import org.vitrivr.cottontail.database.column.mapdb.MapDBColumn
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.TransactionStatus
import org.vitrivr.cottontail.database.general.begin
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTransaction
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.database.queries.components.ComparisonOperator
import org.vitrivr.cottontail.database.queries.components.Predicate
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.TransactionException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.StampedLock
import java.util.stream.Collectors

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
 * @version 1.5
 */
class Entity(override val name: Name.EntityName, override val parent: Schema) : DBO {

    /** The [Path] to the [Entity]'s main folder. */
    override val path: Path = this.parent.path.resolve("entity_${name.simple}")

    /** Internal reference to the [StoreWAL] underpinning this [Entity]. */
    private val store: CottontailStoreWAL = try {
        CottontailStoreWAL.make(
            file = this.path.resolve(FILE_CATALOGUE).toString(),
            volumeFactory = this.parent.parent.config.memoryConfig.volumeFactory,
            allocateIncrement = 1L shl this.parent.parent.config.memoryConfig.dataPageShift,
            fileLockWait = this.parent.parent.config.lockTimeout
        )
    } catch (e: DBException) {
        throw DatabaseException("Failed to open entity '$name': ${e.message}'.")
    }

    /** The header of this [Entity]. */
    private val header: EntityHeader
        get() = this.store.get(HEADER_RECORD_ID, EntityHeaderSerializer)
                ?: throw DatabaseException.DataCorruptionException("Failed to open header of entity '$name'!")

    /** An internal lock that is used to synchronize concurrent read & write access to this [Entity] by different [Entity.Tx]. */
    private val txLock = StampedLock()

    /** An internal lock that is used to synchronize access to this [Entity] and [Entity.Tx] and it being closed or dropped. */
    private val closeLock = StampedLock()

    /** An internal lock that is used to synchronize structural changes to an [Entity]'s indexes (i.e. adding, dropping). */
    private val indexLock = StampedLock()

    /** List of all the [Column]s associated with this [Entity]. */
    private val columns: Map<Name.ColumnName, Column<*>> = this.header.columns.map {
        val n = this.name.column(this.store.get(it, Serializer.STRING)  ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$name': Could not read column definition at position $it!"))
        n to MapDBColumn<Value>(n, this)
    }.toMap()

    /** List of all the [Index]es associated with this [Entity]. */
    private val indexes: MutableCollection<Index> = this.header.indexes.map { idx ->
        val index = this.store.get(idx, IndexEntrySerializer) ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$name': Could not read index definition at position $idx!")
        index.type.open(this.name.index(index.name), this, index.columns.map { col ->
            if (col.contains(".")) {
                /** TODO: For backwards compatibility; remove in future version. */
                this.columnForName(this.name.column(col.split(".").last()))
                        ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$name': It hosts an index for column '$col' that does not exist on the entity!")
            } else {
                this.columnForName(this.name.column(col))
                        ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$name': It hosts an index for column '$col' that does not exist on the entity!")
            }
        }.toTypedArray())
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
        get() = this.header.let { EntityStatistics(it.columns.size, it.size, this.columns.values.first().maxTupleId) }

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
    fun allColumns(): Collection<ColumnDef<*>> = this.closeLock.read {
        check(!this.closed) { "Entity ${this.name} has been closed and cannot be used anymore." }
        this.columns.values.map { it.columnDef }
    }

    /**
     * Returns the [ColumnDef] for the specified [Name.ColumnName].
     *
     * @param name The [Name.ColumnName] of the [Column].
     * @return [ColumnDef] of the [Column].
     */
    fun columnForName(name: Name.ColumnName): ColumnDef<*>? = this.closeLock.read {
        check(!this.closed) { "Entity ${this.name} has been closed and cannot be used anymore." }
        this.columns[name]?.columnDef
    }

    /**
     * Returns all [Index]es for this [Entity].
     *
     * @return Collection of [Index].
     */
    fun allIndexes(): Collection<Index> = this.closeLock.read {
        check(!this.closed) { "Entity ${this.name} has been closed and cannot be used anymore." }
        this.indexLock.read {
            this.indexes
        }
    }

    /**
     * Checks, if this [Entity] has an index for the given [ColumnDef] and (optionally) of the given [IndexType]
     *
     * @param column The [ColumnDef] for which to check.
     * @param type The [IndexType] for which to check.
     * @return True if this [Entity] has an [Index] that satisfies the condition, false otherwise.
     */
    fun hasIndexForColumn(column: ColumnDef<*>, type: IndexType? = null): Boolean = this.closeLock.read {
        check(!this.closed) { "Entity ${this.name} has been closed and cannot be used anymore." }
        this.indexLock.read {
            this.indexes.find { it.columns.contains(column) && (type == null || it.type == type) } != null
        }
    }

    /**
     * Creates the [Index] with the given settings
     *
     * @param name [Name.IndexName] of the [Index] to create.
     * @param type Type of the [Index] to create.
     * @param columns The list of [columns] to [Index].
     */
    fun createIndex(name: Name.IndexName, type: IndexType, columns: Array<ColumnDef<*>>, params: Map<String, String> = emptyMap()) = this.closeLock.read {
        /* Create new index. */
        check(!this.closed) { "Entity ${this.name} has been closed and cannot be used anymore." }
        this.indexLock.write {
            val indexEntry = this.header.indexes.map {
                Pair(it, this.store.get(it, IndexEntrySerializer)
                        ?: throw DatabaseException.DataCorruptionException("Failed to create index '$name': Could not read index definition at position $it!"))
            }.find { this.name.index(it.second.name) == name }

            if (indexEntry != null) throw DatabaseException.IndexAlreadyExistsException(name)

            /* Creates and opens the index. */
            val newIndex = type.create(name, this, columns, params)
            this.indexes.add(newIndex)

            /* Update catalogue + header. */
            try {
                /* Update catalogue. */
                val sid = this.store.put(IndexEntry(name.simple, type, false, columns.map { it.name.simple }.toTypedArray()), IndexEntrySerializer)

                /* Update header. */
                val new = this.header.let { EntityHeader(it.size, it.created, System.currentTimeMillis(), it.columns, it.indexes.copyOf(it.indexes.size + 1)) }
                new.indexes[new.indexes.size - 1] = sid
                this.store.update(Entity.HEADER_RECORD_ID, new, EntityHeaderSerializer)
                this.store.commit()
            } catch (e: DBException) {
                this.store.rollback()
                val pathsToDelete = Files.walk(newIndex.path).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
                pathsToDelete.forEach { Files.delete(it) }
                throw DatabaseException("Failed to create index '$name' due to a storage exception: ${e.message}")
            }
        }
    }


    /**
     * Drops the [Index] with the given name.
     *
     * @param name [Name.IndexName] of the [Index] to drop.
     */
    fun dropIndex(name: Name.IndexName) = this.closeLock.read {
        check(!this.closed) { "Entity ${this.name} has been closed and cannot be used anymore." }
        this.indexLock.write {
            val indexEntry = this.header.indexes.map {
                Pair(it, this.store.get(it, IndexEntrySerializer) ?: throw DatabaseException.DataCorruptionException("Failed to drop index '$name': Could not read index definition at position $it!"))
            }.find { this.name.index(it.second.name) == name }?.let { ie ->
                Triple(ie.first, ie.second, this.indexes.find { it.name == this.name.index(ie.second.name) })
            } ?: throw DatabaseException.IndexDoesNotExistException(name)

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
                throw DatabaseException("Failed to drop index '$name' due to a storage exception: ${e.message}")
            }

            /* Delete files that belong to the index. */
            if (indexEntry.third != null) {
                val pathsToDelete = Files.walk(indexEntry.third!!.path).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
                pathsToDelete.forEach { Files.delete(it) }
            }
        }
    }

    /**
     * Updates the [Index] with the given name.
     *
     * @param name The [Name.IndexName] of the [Index]
     */
    fun updateIndex(name: Name.IndexName) = Tx(readonly = false).begin { tx ->
        val itx = tx.index(name)
        if (itx != null) {
            itx.rebuild()
        } else {
            throw DatabaseException.IndexDoesNotExistException(name)
        }
        true
    }

    /**
     * Updates all [Index]es for this [Entity].
     */
    fun updateAllIndexes() = Tx(readonly = false).begin { tx ->
        tx.indexes().forEach { itx ->
            itx.rebuild()
        }
        true
    }

    /**
     * Closes the [Entity]. Closing an [Entity] is a delicate matter since ongoing [Entity.Tx] objects as well as all involved [Column]s are involved.
     * Therefore, access to the method is mediated by an global [Entity] wide lock.
     */
    override fun close() = this.closeLock.write {
        if (!this.closed) {
            this.columns.values.forEach { it.close() }
            this.store.close()
            this.closed = true
        }
    }

    /**
     * Handles finalization, in case the Garbage Collector reaps a cached [Entity] soft-reference.
     */
    @Synchronized
    protected fun finalize() {
        this.close()
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
    inner class Tx(override val readonly: Boolean, override val tid: UUID = UUID.randomUUID(), omitIndex: Boolean = false) : EntityTransaction {

        /** Obtains a global (non-exclusive) read-lock on [Entity]. Prevents enclosing [Entity] from being closed. */
        private val closeStamp = this@Entity.closeLock.readLock()

        /** Obtains transaction lock on [Entity]. Prevents concurrent read & write access to the enclosing [Entity]. */
        private val txStamp = if (this.readonly) {
            this@Entity.txLock.readLock()
        } else {
            this@Entity.txLock.writeLock()
        }

        /** Map of [ColumnTransaction]s associated with this [Entity.Tx]; order of [ColumnDef] is preserved since a LinkedHashMap is used. */
        private val colTxs: Map<ColumnDef<*>, ColumnTransaction<*>> = this@Entity.columns.values.map { it.columnDef to it.newTransaction(this.readonly, tid) }.toMap()

        /** List of [IndexTransaction] associated with this [Entity.Tx]. */
        private val indexTxs: Collection<IndexTransaction> = if (!omitIndex) {
            this@Entity.indexes.map { it.begin(this) }
        } else {
            emptyList()
        }

        /** Flag indicating this [Entity.Tx]'s current status. */
        @Volatile
        override var status: TransactionStatus = TransactionStatus.CLEAN
            private set

        /** Reference to the [Entity]. */
        override val entity: Entity
            get() = this@Entity

        /** Tries to acquire a global read-lock on this [Entity]. */
        init {
            if (this@Entity.closed) {
                throw TransactionException.TransactionDBOClosedException(tid)
            }
        }

        /**
         * A [StampedLock] local to this [Entity.Tx].
         *
         * It assures that this [Entity] cannot be committed, closed or rolled back while it is being used.
         */
        private val localLock = StampedLock()

        /**
         * Commits all changes made through this [Entity.Tx] since the last commit or rollback.
         */
        override fun commit() = this.localLock.write {
            if (this.status == TransactionStatus.DIRTY) {
                this.colTxs.forEach { it.value.commit() }
                this@Entity.store.commit()
                this.status = TransactionStatus.CLEAN
            }
            this.indexTxs.forEach { it.commit() }
        }

        /**
         * Rolls all changes made through this [Entity.Tx] back to the last commit.
         */
        override fun rollback() = this.localLock.write {
            if (this.status == TransactionStatus.DIRTY) {
                this.colTxs.forEach { it.value.rollback() }
                this@Entity.store.rollback()
                this.status = TransactionStatus.CLEAN
            }
        }

        /**
         * Closes this [Entity.Tx] and thereby releases all the [Column.Tx] and the global lock. Closed [Entity.Tx] cannot be used anymore!
         */
        override fun close() = this.localLock.write {
            if (this.status != TransactionStatus.CLOSED) {
                if (this.status == TransactionStatus.DIRTY) {
                    this.rollback()
                }
                this.indexTxs.forEach { it.close() }
                this.colTxs.forEach { it.value.close() }
                this.status = TransactionStatus.CLOSED
                this@Entity.txLock.unlock(this.txStamp)
                this@Entity.closeLock.unlockRead(this.closeStamp)
            }
        }

        /**
         * Reads the values of one or many [Column]s and returns it as a [Tuple]
         *
         * @param tupleId The [TupleId] of the desired entry.
         * @param columns The [ColumnDef]s that should be read.
         *
         * @return The desired [Record].
         *
         * @throws DatabaseException If tuple with the desired ID doesn't exist OR is invalid.
         */
        fun read(tupleId: TupleId, columns: Array<ColumnDef<*>>): Record = this.localLock.read {
            checkValidForRead()
            checkValidTupleId(tupleId)

            /* Read values. */
            val values = columns.map {
                checkColumnsExist(it)
                this.colTxs.getValue(it).read(tupleId)
            }.toTypedArray()

            /* Return value of all the desired columns. */
            return StandaloneRecord(tupleId, columns, values)
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
         * Returns the maximum tuple ID occupied by entries in this [Entity].
         *
         * @return The maximum tuple ID occupied by entries in this [Entity].
         */
        fun maxTupleId(): TupleId = this.localLock.read {
            checkValidForRead()
            return this@Entity.columns.values.first().maxTupleId
        }

        /**
         * Creates and returns a new [CloseableIterator] for this [Entity.Tx] that returns
         * all [TupleId]s contained within the surrounding [Entity].
         *
         * <strong>Important:</strong> It remains to the caller to close the [CloseableIterator]
         *
         * @param columns The [ColumnDef]s that should be scanned.
         *
         * @return [CloseableIterator]
         */
        override fun scan(columns: Array<ColumnDef<*>>): CloseableIterator<Record> = scan(columns, 1L..this.maxTupleId())

        /**
         * Creates and returns a new [CloseableIterator] for this [Entity.Tx] that returns all [TupleId]s
         * contained within the surrounding [Entity] and a certain range.
         *
         * <strong>Important:</strong> It remains to the caller to close the [CloseableIterator]
         *
         * @param columns The [ColumnDef]s that should be scanned.
         * @param range The [LongRange] that should be scanned.
         *
         * @return [CloseableIterator]
         */
        override fun scan(columns: Array<ColumnDef<*>>, range: LongRange) = object : CloseableIterator<Record> {
            init {
                checkValidForRead()
            }


            /** Acquires a read lock for the surrounding [Entity.Tx]*/
            private val lock = this@Tx.localLock.readLock()

            /** The wrapped [CloseableIterator] of the first (primary) column. */
            private val wrapped = this@Tx.colTxs.entries.first().value.scan(range)

            /** Flag indicating whether this [CloseableIterator] has been closed. */
            @Volatile
            private var closed = false

            /**
             * Returns the next element in the iteration.
             */
            override fun next(): Record {
                check(!this.closed) { "Illegal invocation of next(): This CloseableIterator has been closed." }
                return this@Tx.read(this.wrapped.next(), columns)
            }

            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean {
                check(!this.closed) { "Illegal invocation of hasNext(): This CloseableIterator has been closed." }
                return this.wrapped.hasNext()
            }

            /**
             * Closes this [CloseableIterator] and releases all locks and resources associated with it.
             */
            override fun close() {
                if (!this.closed) {
                    this.wrapped.close()
                    this@Tx.localLock.unlock(this.lock)
                    this.closed = true
                }
            }
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
                (columns?.all { tx.columns.contains(it) }
                        ?: true) && (type == null || tx.type == type)
            }
        }

        /**
         * Returns the [IndexTransaction] for the given [Name] or null, if such a [IndexTransaction] doesn't exist.
         *
         * @param name The [Name] of the [Index] the [IndexTransaction] belongs to.
         * @return Optional [IndexTransaction]
         */
        override fun index(name: Name.IndexName): IndexTransaction? = this.localLock.read {
            this.indexTxs.find { it.name == name }
        }

        /**
         * Insert the provided [Record]. Columns specified in the [Record] that are not part
         * of the [Entity] will cause an error! This will set this [Entity.Tx] to [TransactionStatus.DIRTY].
         *
         * @param record The [Record] that should be inserted.
         * @return The ID of the record or null, if nothing was inserted.
         * @throws TransactionException If some of the sub-transactions on [Column] level caused an error.
         * @throws DatabaseException If a general database error occurs during the insert.
         */
        override fun insert(record: Record): TupleId? = this.localLock.read {
            checkValidForWrite()

            /* Perform sanity check; all columns held by this entity must be contained in the record. */
            this.entity.allColumns().forEach {
                if (!record.columns.contains(it)) {
                    throw TransactionException.TransactionValidationException(this.tid, "The provided record is missing the column $it, which is required for an insert.")
                }
            }

            try {
                var lastRecId: Long? = null
                record.columns.zip(record.values) { columnDef, value ->
                    columnDef.validateOrThrow(value)
                    val recId = (this.colTxs.getValue(columnDef) as ColumnTransaction<Value>).insert(value)
                    if (lastRecId != recId && lastRecId != null) {
                        throw DatabaseException.DataCorruptionException("Entity '${this@Entity.name}' is corrupt. Insert did not yield same record ID for all columns involved!")
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
         * Updates the provided [Record] (identified based on its [TupleId]). Columns specified in the [Record] that are not part
         * of the [Entity] will cause an error! This will set this [Entity.Tx] to [TransactionStatus.DIRTY].
         *
         * @param record The [Record] that should be updated
         *
         * @throws DatabaseException If an error occurs during the insert.
         */
        override fun update(record: Record) = this.localLock.read {
            checkValidForWrite()
            checkColumnsExist(*record.columns)
            try {
                record.columns.zip(record.values) { columnDef, value ->
                    columnDef.validateOrThrow(value)
                    (this.colTxs.getValue(columnDef) as ColumnTransaction<Value>).update(record.tupleId, value)
                }
                Unit
            } catch (e: DatabaseException) {
                this.status = TransactionStatus.ERROR
                throw e
            } catch (e: DBException) {
                this.status = TransactionStatus.ERROR
                throw DatabaseException("Updating record ${record.tupleId} failed due to an error in the underlying storage: ${e.message}.")
            }
        }

        /**
         * Deletes the entry with the provided [TupleId]. This will set this [Entity.Tx] to [TransactionStatus.DIRTY]
         *
         * @param tupleId The ID of the [Tuple] that should be deleted.
         *
         * @throws DatabaseException If an error occurs during the insert.
         */
        override fun delete(tupleId: Long) = this.localLock.read {
            checkValidForWrite()
            try {
                /* Perform delete on each column. */
                this.colTxs.values.forEach { it.delete(tupleId) }

                /* Update header. */
                val header = this@Entity.header
                header.size -= 1
                header.modified = System.currentTimeMillis()
                this@Entity.store.update(HEADER_RECORD_ID, header, EntityHeaderSerializer)
            } catch (e: DBException) {
                this.status = TransactionStatus.ERROR
                throw DatabaseException("Deleting record $tupleId failed due to an error in the underlying storage: ${e.message}.")
            }
        }

        /**
         * Check if all the provided [ColumnDef]s exist on this [Entity] and that they have the type that was expected!
         *
         * @params The list of [ColumnDef]s that should be checked.
         */
        private fun checkColumnsExist(vararg columns: ColumnDef<*>) = columns.forEach { it1 ->
            if (!this.colTxs.containsKey(it1)) {
                throw TransactionException.ColumnUnknownException(this.tid, it1)
            }
        }

        /**
         * Checks if the provided tupleID is valid. Otherwise, an exception will be thrown.
         *
         * @param tupleId The tuple ID to check.
         */
        private fun checkValidTupleId(tupleId: Long) {
            if (tupleId <= HEADER_RECORD_ID) {
                throw TransactionException.InvalidTupleId(tid, tupleId)
            }
        }

        /**
         * Checks if this [Entity.Tx] is in a valid state for read operations to happen.
         */
        private fun checkValidForRead() {
            if (this.status == TransactionStatus.CLOSED) throw TransactionException.TransactionClosedException(tid)
            if (this.status == TransactionStatus.ERROR) throw TransactionException.TransactionInErrorException(tid)
        }

        /**
         * Checks if this [Entity.Tx] is in a valid state for write operations to happen.
         */
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
