package org.vitrivr.cottontail.database.entity

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.mapdb.CottontailStoreWAL
import org.mapdb.DBException
import org.mapdb.Serializer
import org.mapdb.StoreWAL
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.column.ColumnTx
import org.vitrivr.cottontail.database.column.mapdb.MapDBColumn
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.TxStatus
import org.vitrivr.cottontail.database.index.BrokenIndex
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.TxException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.utilities.extensions.write
import java.io.IOException
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardCopyOption
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
 * @version 1.6.0
 */
class Entity(override val name: Name.EntityName, override val parent: Schema) : DBO {
    /**
     * Companion object of the [Entity]
     */
    companion object {
        /** Filename for the [Entity] catalogue.  */
        const val FILE_CATALOGUE = "index.db"

        /** Filename for the [Entity] catalogue.  */
        const val HEADER_RECORD_ID = 1L

        /** Entity wide LOGGER instance. */
        private val LOGGER = LoggerFactory.getLogger(Entity::class.java)
    }

    /** The [Path] to the [Entity]'s main folder. */
    override val path: Path = this.parent.path.resolve("entity_${name.simple}")

    /** Internal reference to the [StoreWAL] underpinning this [Entity]. */
    private val store: CottontailStoreWAL = try {
        this.parent.parent.config.mapdb.store(this.path.resolve(FILE_CATALOGUE))
    } catch (e: DBException) {
        throw DatabaseException("Failed to open entity '$name': ${e.message}'.")
    }

    /** The header of this [Entity]. */
    private val header: EntityHeader
        get() = this.store.get(HEADER_RECORD_ID, EntityHeader.Serializer)
            ?: throw DatabaseException.DataCorruptionException("Failed to open header of entity '$name'!")

    /** An internal lock that is used to synchronize access to this [Entity] and [Entity.Tx] and it being closed or dropped. */
    private val closeLock = StampedLock()

    /** List of all the [Column]s associated with this [Entity]; Iteration order of entries as defined in schema! */
    private val columns: MutableMap<Name.ColumnName, Column<*>> = Object2ObjectLinkedOpenHashMap()

    /** List of all the [Index]es associated with this [Entity]. */
    private val indexes: MutableMap<Name.IndexName, Index> = Object2ObjectOpenHashMap()

    init {
        /* Initialize columns. */
        this.header.columns.map {
            val columnName = this.name.column(
                this.store.get(it, Serializer.STRING)
                    ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$name': Could not read column definition at position $it!")
            )
            this.columns[columnName] = MapDBColumn<Value>(columnName, this)
        }

        /* Initialize indexes. */
        this.header.indexes.forEach { idx ->
            val indexEntry = this.store.get(idx, IndexEntry.Serializer)
                ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$name': Could not read index definition at position $idx!")
            val indexName = this.name.index(indexEntry.name)
            try {
                this.indexes[indexName] =
                    indexEntry.type.open(indexName, this, indexEntry.columns.map { col ->
                        this.columns[this.name.column(col)]?.columnDef
                            ?: throw DatabaseException.DataCorruptionException("Column '$col' does not exist on the entity!")
                    }.toTypedArray())
            } catch (e: Throwable) {
                if (this.parent.parent.config.allowBrokenIndex) {
                    LOGGER.warn("Index $indexName could not be opened and has been marked as broken. Try to optimize entity ${this.name} to resolve the issue...")
                    this.indexes[indexName] = BrokenIndex(
                        indexName,
                        this,
                        indexEntry.type,
                        this.path.resolve(indexEntry.name)
                    )
                } else {
                    throw DatabaseException.DataCorruptionException("Failed to load index '$indexName': ${e.message}. Start Cottontail DB with 'allowBrokenIndex' option and retry!")
                }
            }
        }
    }

    /**
     * Status indicating whether this [Entity] is open or closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    /**
     * Creates and returns a new [Entity.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [Entity.Tx] for.
     * @return New [Entity.Tx]
     */
    override fun newTx(context: TransactionContext) = this.Tx(context)

    /**
     * Creates and returns an [EntityStatistics] snapshot.
     *
     * @return [EntityStatistics] for this [Entity].
     */
    val statistics: EntityStatistics
        get() = this.header.let {
            EntityStatistics(
                it.columns.size,
                it.size,
                this.columns.values.first().maxTupleId
            )
        }

    /**
     * Returns all [ColumnDef]s for this [Entity].
     *
     * This operation takes place outside of any transaction context, hence, no isolation guarantees
     * can be given for the results returned.
     *
     * @return List of all [ColumnDef]s.
     */
    @Deprecated("Use Entity.Tx.listAllColumns() instead.")
    fun allColumns(): List<ColumnDef<*>> {
        return this.columns.values.map { it.columnDef }
    }

    /**
     * Returns all [Index]s for this [Entity].
     *
     * This operation takes place outside of any transaction context, hence, no isolation guarantees
     * can be given for the results returned.
     *
     * @return List of all [Column]s.
     */
    @Deprecated("Use Entity.Tx.listAllIndexes() instead.")
    fun allIndexes(): List<Index> {
        return this.indexes.values.toList()
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
        if (!this.closed) {
            this.close()
        }
    }

    /**
     * A [Tx] that affects this [Entity]. Opening a [Entity.Tx] will automatically spawn [ColumnTx]
     * and [IndexTx] for every [Column] and [IndexTx] associated with this [Entity].
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    inner class Tx(context: TransactionContext) : AbstractTx(context), EntityTx {

        /** Obtains a global (non-exclusive) read-lock on [Entity]. Prevents enclosing [Entity] from being closed. */
        private val closeStamp = this@Entity.closeLock.readLock()

        /** Actions that should be executed after committing this [Tx]. */
        private val postCommitAction = mutableListOf<Runnable>()

        /** Actions that should be executed after rolling back this [Tx]. */
        private val postRollbackAction = mutableListOf<Runnable>()

        /** A snapshot of the surrounding [Entity]'s header. */
        private val header: EntityHeader by lazy { this@Entity.header }

        /** Reference to the surrounding [Entity]. */
        override val dbo: Entity
            get() = this@Entity

        /** Tries to acquire a global read-lock on this entity. */
        init {
            if (this@Entity.closed) {
                throw TxException.TxDBOClosedException(this.context.txId)
            }
        }

        /**
         * Reads the values of one or many [Column]s and returns it as a [Record]
         *
         * @param tupleId The [TupleId] of the desired entry.
         * @param columns The [ColumnDef]s that should be read.
         * @return The desired [Record].
         *
         * @throws DatabaseException If tuple with the desired ID doesn't exist OR is invalid.
         */
        override fun read(tupleId: TupleId, columns: Array<ColumnDef<*>>): Record = this.withReadLock {
            /* Read values from underlying columns. */
            val values = columns.map {
                val column = this@Entity.columns[it.name]
                        ?: throw IllegalArgumentException("Column $it does not exist on entity ${this@Entity.name}.")
                (this.context.getTx(column) as ColumnTx<*>).read(tupleId)
            }.toTypedArray()

            /* Return value of all the desired columns. */
            return StandaloneRecord(tupleId, columns, values)
        }

        /**
         * Returns the number of entries in this [Entity].
         *
         * @return The number of entries in this [Entity].
         */
        override fun count(): Long = this.withReadLock {
            return this@Entity.header.size
        }

        /**
         * Returns the maximum tuple ID occupied by entries in this [Entity].
         *
         * @return The maximum tuple ID occupied by entries in this [Entity].
         */
        override fun maxTupleId(): TupleId = this.withReadLock {
            return this@Entity.columns.values.first().maxTupleId
        }

        /**
         * Lists all [Column]s for the [Entity] associated with this [EntityTx].
         *
         * @return List of all [Column]s.
         */
        override fun listColumns(): List<Column<*>> = this.withReadLock {
            return this@Entity.columns.values.toList()
        }

        /**
         * Returns the [ColumnDef] for the specified [Name.ColumnName].
         *
         * @param name The [Name.ColumnName] of the [Column].
         * @return [ColumnDef] of the [Column].
         */
        override fun columnForName(name: Name.ColumnName): Column<*> = this.withReadLock {
            if (!name.wildcard) {
                this@Entity.columns[name] ?: throw DatabaseException.ColumnDoesNotExistException(
                    name
                )
            } else {
                val fqn = this@Entity.name.column(name.simple)
                this@Entity.columns[fqn] ?: throw DatabaseException.ColumnDoesNotExistException(fqn)
            }
        }

        /**
         * Lists all [Index] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        override fun listIndexes(): List<Index> = this.withReadLock {
            return this@Entity.indexes.values.toList()
        }

        /**
         * Lists [Name.IndexName] for all [Index] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        override fun indexForName(name: Name.IndexName): Index = this.withReadLock {
            this@Entity.indexes[name] ?: throw DatabaseException.IndexDoesNotExistException(name)
        }

        /**
         * Creates the [Index] with the given settings
         *
         * @param name [Name.IndexName] of the [Index] to create.
         * @param type Type of the [Index] to create.
         * @param columns The list of [columns] to [Index].
         */
        override fun createIndex(
            name: Name.IndexName,
            type: IndexType,
            columns: Array<ColumnDef<*>>,
            params: Map<String, String>
        ): Index = this.withWriteLock {
            if (this@Entity.indexes.containsKey(name)) {
                throw DatabaseException.IndexAlreadyExistsException(name)
            }

            /* Creates and opens the index and schedules a ROLLBACK action in case of failure. */
            val newIndex = type.create(name, this.dbo, columns, params)

            /* ON COMMIT: Make index available. */
            this.postCommitAction.add {
                this@Entity.indexes[name] = newIndex
            }

            /* ON ROLLBACK: Remove index data. */
            this.postRollbackAction.add {
                newIndex.close()
                val pathsToDelete = Files.walk(newIndex.path).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
                pathsToDelete.forEach { Files.delete(it) }
            }

            /* Update catalogue + header. */
            try {
                /* Update catalogue. */
                val sid = this@Entity.store.put(
                    IndexEntry(
                        name.simple,
                        type,
                        false,
                        columns.map { it.name.simple }.toTypedArray()
                    ), IndexEntry.Serializer
                )

                /* Update header. */
                val new = this.header.let {
                    EntityHeader(
                        it.size,
                        it.created,
                        System.currentTimeMillis(),
                        it.columns,
                        it.indexes.copyOf(it.indexes.size + 1)
                    )
                }
                new.indexes[new.indexes.size - 1] = sid
                this@Entity.store.update(HEADER_RECORD_ID, new, EntityHeader.Serializer)
                return newIndex
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Failed to create index '$name' due to a storage exception: ${e.message}")
            }
        }

        /**
         * Drops the [Index] with the given name.
         *
         * @param name [Name.IndexName] of the [Index] to drop.
         */
        override fun dropIndex(name: Name.IndexName) = this.withWriteLock {
            /* Obtain index and acquire exclusive lock on it. */
            val index = this.indexForName(name)
            val indexRecId = this@Entity.header.indexes.find { this@Entity.store.get(it, Serializer.STRING) == name.simple }
                    ?: throw DatabaseException.DataCorruptionException("Could not find RecId for index $name.")

            if (this.context.lockOn(index) == LockMode.NO_LOCK) {
                this.context.requestLock(index, LockMode.EXCLUSIVE)
            }

            /* Close index and remove it from registry. */
            index.close()
            this@Entity.indexes.remove(name)

            /* Update header. */
            try {

                try {
                    /*
                     * Rename index file / folder:
                     *
                     * if these have been removed already (sometimes necessary for manual recovery),
                     * this is not a failure scenario.
                     */
                    val shadowIndex =
                        index.path.resolveSibling(index.path.fileName.toString() + "~dropped")
                    Files.move(index.path, shadowIndex, StandardCopyOption.ATOMIC_MOVE)

                    /* ON COMMIT: Remove index files. */
                    this.postCommitAction.add {
                        val pathsToDelete =
                            Files.walk(shadowIndex).sorted(Comparator.reverseOrder())
                                .collect(Collectors.toList())
                        pathsToDelete.forEach { Files.delete(it) }
                        this.context.releaseLock(index)
                    }

                    /* ON ROLLBACK: Move back index and re-open it. */
                    this.postRollbackAction.add {
                        Files.move(shadowIndex, index.path, StandardCopyOption.ATOMIC_MOVE)
                    }
                } catch (e: NoSuchFileException) {
                    LOGGER.warn("Files for index '$name' are missing; dropping continues anyways.")
                }

                /* ON ROLLBACK: Move back index and re-open it. */
                this.postRollbackAction.add {
                    val entry = this@Entity.store.get(indexRecId, IndexEntry.Serializer)
                        ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$name': Could not read index definition at position $indexRecId!")
                    val columns = entry.columns.map { col ->
                        if (col.contains(".")) {
                            this@Entity.columns[this@Entity.name.column(
                                col.split(".").last()
                            )]?.columnDef
                                ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$name': It hosts an index for column '$col' that does not exist on the entity!")
                        } else {
                            this@Entity.columns[this@Entity.name.column(col)]?.columnDef
                                ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$name': It hosts an index for column '$col' that does not exist on the entity!")
                        }
                    }.toTypedArray()
                    this@Entity.indexes[name] = index.type.open(name, this@Entity, columns)
                    this.context.releaseLock(index)
                }

                val new = this@Entity.header.let {
                    EntityHeader(
                        it.size,
                        it.created,
                        System.currentTimeMillis(),
                        it.columns,
                        it.indexes.filter { recId -> recId != indexRecId }.toLongArray()
                    )
                }
                this@Entity.store.update(HEADER_RECORD_ID, new, EntityHeader.Serializer)
                this@Entity.store.delete(indexRecId, IndexEntry.Serializer)
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Failed to drop index '$name' due to a storage exception: ${e.message}")
            } catch (e: IOException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Failed to drop index '$name' due to an IO exception: ${e.message}")
            }
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
                this@Tx.withReadLock { /* No op. */ }
            }

            /** The wrapped [CloseableIterator] of the first (primary) column. */
            private val wrapped = (this@Tx.context.getTx(this@Entity.columns.values.first()) as ColumnTx<*>).scan(range)

            /** Flag indicating whether this [CloseableIterator] has been closed. */
            @Volatile
            private var closed = false

            /**
             * Returns the next element in the iteration.
             */
            override fun next(): Record {
                check(!this.closed) { "Illegal invocation of next(): This CloseableIterator has been closed." }

                /* Read values from underlying columns. */
                val tupleId = this.wrapped.next()
                val values = columns.map {
                    val column = this@Entity.columns[it.name]
                            ?: throw IllegalArgumentException("Column $it does not exist on entity ${this@Entity.name}.")
                    (this@Tx.context.getTx(column) as ColumnTx<*>).read(tupleId)
                }.toTypedArray()

                /* Return value of all the desired columns. */
                return StandaloneRecord(tupleId, columns, values)
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
                    this.closed = true
                }
            }
        }

        /**
         * Insert the provided [Record]. This will set this [Entity.Tx] to [TxStatus.DIRTY].
         *
         * @param record The [Record] that should be inserted.
         * @return The ID of the record or null, if nothing was inserted.
         *
         * @throws TxException If some of the [Tx] on [Column] or [Index] level caused an error.
         * @throws DatabaseException If a general database error occurs during the insert.
         */
        override fun insert(record: Record): TupleId? = this.withWriteLock {
            try {
                var lastTupleId: TupleId? = null
                val inserts = Object2ObjectArrayMap<ColumnDef<*>, Value>(this@Entity.columns.size)
                this@Entity.columns.values.forEach {
                    val tx = this.context.getTx(it) as ColumnTx<Value>
                    val value = record[it.columnDef]
                    val tupleId = tx.insert(value)
                    if (lastTupleId != tupleId && lastTupleId != null) {
                        throw DatabaseException.DataCorruptionException("Entity '${this@Entity.name}' is corrupt. Insert did not yield same record ID for all columns involved!")
                    }
                    lastTupleId = tupleId
                    inserts[it.columnDef] = value
                }

                if (lastTupleId != null) {
                    /* Update header (don't persist). */
                    this.header.size += 1

                    /* Issue DataChangeEvent.InsertDataChange event & update indexes. */
                    /* ToDo: System wide event bus for [DataChangeEvent]. */
                    val event = DataChangeEvent.InsertDataChangeEvent(
                        this@Entity.name,
                        lastTupleId!!,
                        inserts
                    )
                    this@Entity.indexes.values.forEach {
                        (this.context.getTx(it) as IndexTx).update(event)
                    }
                }

                return lastTupleId
            } catch (e: DatabaseException) {
                this.status = TxStatus.ERROR
                throw e
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Inserting record failed due to an error in the underlying storage: ${e.message}.")
            }
        }

        /**
         * Updates the provided [Record] (identified based on its [TupleId]). Columns specified in the [Record] that are not part
         * of the [Entity] will cause an error! This will set this [Entity.Tx] to [TxStatus.DIRTY].
         *
         * @param record The [Record] that should be updated
         *
         * @throws DatabaseException If an error occurs during the insert.
         */
        override fun update(record: Record) = this.withWriteLock {
            try {
                val updates =
                    Object2ObjectArrayMap<ColumnDef<*>, Pair<Value?, Value?>>(record.columns.size)
                record.columns.forEach { def ->
                    val column = this@Entity.columns[def.name]
                        ?: throw IllegalArgumentException("Column $def does not exist on entity ${this@Entity.name}.")
                    val value = record[def]
                    val columnTx = (this.context.getTx(column) as ColumnTx<Value>)
                    updates[def] = Pair(
                        columnTx.update(record.tupleId, value),
                        value
                    ) /* Map: ColumnDef -> Pair[Old, New]. */
                }

                /* Issue DataChangeEvent.UpdateDataChangeEvent event & update indexes. */
                /* ToDo: System wide event bus for [DataChangeEvent]. */
                val event =
                    DataChangeEvent.UpdateDataChangeEvent(this@Entity.name, record.tupleId, updates)
                this@Entity.indexes.values.forEach {
                    (this.context.getTx(it) as IndexTx).update(event)
                }
            } catch (e: DatabaseException) {
                this.status = TxStatus.ERROR
                throw e
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Updating record ${record.tupleId} failed due to an error in the underlying storage: ${e.message}.")
            }
        }

        /**
         * Deletes the entry with the provided [TupleId]. This will set this [Entity.Tx] to [TxStatus.DIRTY]
         *
         * @param tupleId The [TupleId] of the entry that should be deleted.
         *
         * @throws DatabaseException If an error occurs during the insert.
         */
        override fun delete(tupleId: TupleId) = this.withWriteLock {
            try {
                /* Perform delete on each column. */
                val deleted = Object2ObjectArrayMap<ColumnDef<*>, Value?>(this@Entity.columns.size)
                this@Entity.columns.values.map {
                    deleted[it.columnDef] = (this.context.getTx(it) as ColumnTx<*>).delete(tupleId)
                }

                /* Update header (don't persist). */
                this.header.size -= 1

                /* Issue DataChangeEvent.DeleteDataChangeEvent event & update indexes. */
                /* ToDo: System wide event bus for [DataChangeEvent]. */
                val event =
                    DataChangeEvent.DeleteDataChangeEvent(this@Entity.name, tupleId, deleted)
                this@Entity.indexes.values.forEach {
                    (this.context.getTx(it) as IndexTx).update(event)
                }
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Deleting record $tupleId failed due to an error in the underlying storage: ${e.message}.")
            }
        }

        /**
         * Performs a COMMIT of all changes made through this [Entity.Tx].
         */
        override fun performCommit() {
            /** Update and persist header + commit store. */
            this.header.modified = System.currentTimeMillis()
            this@Entity.store.update(HEADER_RECORD_ID, this.header, EntityHeader.Serializer)
            this@Entity.store.commit()

            /* Execute post-commit actions. */
            this.postCommitAction.forEach { it.run() }
            this.postRollbackAction.clear()
            this.postCommitAction.clear()
        }

        /**
         * Performs a ROLLBACK of all changes made through this [Entity.Tx].
         */
        override fun performRollback() {
            this@Entity.store.rollback()

            /* Execute post-rollback actions. */
            this.postRollbackAction.forEach { it.run() }
            this.postCommitAction.clear()
            this.postRollbackAction.clear()
        }

        /**
         * Closes all the [ColumnTx] and [IndexTx] and releases the [closeLock] on the [Entity].
         */
        override fun cleanup() {
            this@Entity.closeLock.unlockRead(this.closeStamp)
        }
    }
}
