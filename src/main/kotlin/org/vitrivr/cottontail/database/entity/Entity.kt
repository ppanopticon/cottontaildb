package org.vitrivr.cottontail.database.entity

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.mapdb.CottontailStoreWAL
import org.mapdb.DBException
import org.mapdb.Serializer
import org.mapdb.StoreWAL
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.column.ColumnTx
import org.vitrivr.cottontail.database.column.hare.HareColumn
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.TxStatus
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
        get() = this.store.get(HEADER_RECORD_ID, EntityHeaderSerializer)
                ?: throw DatabaseException.DataCorruptionException("Failed to open header of entity '$name'!")

    /** An internal lock that is used to synchronize access to this [Entity] and [Entity.Tx] and it being closed or dropped. */
    private val closeLock = StampedLock()

    /** List of all the [Column]s associated with this [Entity]; Iteration order of entries as defined in schema! */
    private val columns: MutableMap<Name.ColumnName, Column<*>> = Collections.synchronizedMap(Object2ObjectLinkedOpenHashMap())

    /** List of all the [Index]es associated with this [Entity]. */
    private val indexes: MutableMap<Name.IndexName, Index> = Collections.synchronizedMap(Object2ObjectOpenHashMap())

    init {
        /* Initialize columns. */
        this.header.columns.map {
            val columnName = this.name.column(this.store.get(it, Serializer.STRING)
                    ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$name': Could not read column definition at position $it!"))
            this.columns[columnName] = HareColumn<Value>(columnName, this)
        }

        /* Initialize indexes. */
        this.header.indexes.map { idx ->
            val indexEntry = this.store.get(idx, IndexEntrySerializer)
                    ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$name': Could not read index definition at position $idx!")
            val indexName = this.name.index(indexEntry.name)
            val index = indexEntry.type.open(indexName, this, indexEntry.columns.map { col ->
                if (col.contains(".")) {
                    this.columns[this.name.column(col.split(".").last())]?.columnDef
                            ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$name': It hosts an index for column '$col' that does not exist on the entity!")
                } else {
                    this.columns[this.name.column(col)]?.columnDef
                            ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$name': It hosts an index for column '$col' that does not exist on the entity!")
                }
            }.toTypedArray())
            this.indexes[indexName] = index
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
        get() = this.header.let { EntityStatistics(it.columns.size, it.size, this.header.maxTupleId) }

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

        /** Reference to the surrounding [Entity]. */
        override val dbo: Entity
            get() = this@Entity

        /** Tries to acquire a global read-lock on this [Entity]. */
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
            return this@Entity.statistics.maxTupleId
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
            if (name.fqn) {
                this@Entity.columns[name]
                        ?: throw DatabaseException.ColumnDoesNotExistException(name)
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
        override fun createIndex(name: Name.IndexName, type: IndexType, columns: Array<ColumnDef<*>>, params: Map<String, String>) = this.withWriteLock {
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
                val pathsToDelete = Files.walk(newIndex.path).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
                pathsToDelete.forEach { Files.delete(it) }
            }

            /* Update catalogue + header. */
            try {
                /* Update catalogue. */
                val sid = this@Entity.store.put(IndexEntry(name.simple, type, false, columns.map { it.name.simple }.toTypedArray()), IndexEntrySerializer)

                /* Update header. */
                val new = this@Entity.header.let { EntityHeader(it.size, it.created, it.maxTupleId, System.currentTimeMillis(), it.columns, it.indexes.copyOf(it.indexes.size + 1)) }
                new.indexes[new.indexes.size - 1] = sid
                this@Entity.store.update(HEADER_RECORD_ID, new, EntityHeaderSerializer)
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
                /* Rename index file / folder. */
                val shadowIndex = index.path.resolveSibling(index.path.fileName.toString() + "~dropped")
                Files.move(index.path, shadowIndex)

                /* ON COMMIT: Remove index files. */
                this.postCommitAction.add {
                    val pathsToDelete = Files.walk(index.path).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
                    pathsToDelete.forEach { Files.delete(it) }
                    this.context.releaseLock(index)
                }

                /* ON ROLLBACK: Move back index and re-open it. */
                this.postRollbackAction.add {
                    Files.move(shadowIndex, index.path)
                    val entry = this@Entity.store.get(indexRecId, IndexEntrySerializer)
                            ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$name': Could not read index definition at position $indexRecId!")
                    val columns = entry.columns.map { col ->
                        if (col.contains(".")) {
                            this@Entity.columns[this@Entity.name.column(col.split(".").last())]?.columnDef
                                    ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$name': It hosts an index for column '$col' that does not exist on the entity!")
                        } else {
                            this@Entity.columns[this@Entity.name.column(col)]?.columnDef
                                    ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$name': It hosts an index for column '$col' that does not exist on the entity!")
                        }
                    }.toTypedArray()
                    this@Entity.indexes[name] = index.type.open(name, this@Entity, columns)
                    this.context.releaseLock(index)
                }

                val new = this@Entity.header.let { EntityHeader(it.size, it.created, it.maxTupleId, System.currentTimeMillis(), it.columns, it.indexes.filter { it != indexRecId }.toLongArray()) }
                this@Entity.store.update(HEADER_RECORD_ID, new, EntityHeaderSerializer)
                this@Entity.store.delete(indexRecId, IndexEntrySerializer)
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
            override var isOpen = true
                private set

            /**
             * Returns the next element in the iteration.
             */
            override fun next(): Record {
                check(this.isOpen) { "Illegal invocation of next(): This CloseableIterator has been closed." }

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
                check(this.isOpen) { "Illegal invocation of hasNext(): This CloseableIterator has been closed." }
                return this.wrapped.hasNext()
            }

            /**
             * Closes this [CloseableIterator] and releases all locks and resources associated with it.
             */
            override fun close() {
                if (this.isOpen) {
                    this.wrapped.close()
                    this.isOpen = true
                }
            }
        }

        /**
         * Insert the provided [Record]. Columns specified in the [Record] that are not part
         * of the [Entity] will cause an error! This will set this [Entity.Tx] to [TxStatus.DIRTY].
         *
         * @param record The [Record] that should be inserted.
         * @return The ID of the record or null, if nothing was inserted.
         * @throws TxException If some of the sub-transactions on [Column] level caused an error.
         * @throws DatabaseException If a general database error occurs during the insert.
         */
        override fun insert(record: Record): TupleId? = this.withWriteLock {
            /* Perform sanity check; all columns held by this entity must be contained in the record. */
            this.listColumns().forEach {
                if (!record.columns.contains(it.columnDef)) {
                    throw TxException.TxValidationException(this.context.txId, "The provided record is missing the column $it, which is required for an insert.")
                }
            }

            try {
                var lastTupleIdId: TupleId? = null
                record.forEach { columnDef, value ->
                    val column = this@Entity.columns[columnDef.name]
                            ?: throw IllegalArgumentException("Column $columnDef does not exist on entity ${this@Entity.name}.")
                    val tupleId = (this.context.getTx(column) as ColumnTx<Value>).insert(value)
                    if (lastTupleIdId != tupleId && lastTupleIdId != null) {
                        throw DatabaseException.DataCorruptionException("Entity '${this@Entity.name}' is corrupt. Insert did not yield same record ID for all columns involved!")
                    }
                    lastTupleIdId = tupleId
                }

                /* Update the header of this entity. */
                if (lastTupleIdId != null) {
                    val header = this@Entity.header
                    header.size += 1
                    header.modified = System.currentTimeMillis()
                    this@Entity.store.update(HEADER_RECORD_ID, header, EntityHeaderSerializer)
                }

                return lastTupleIdId
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
                record.forEach { columnDef, value ->
                    val column = this@Entity.columns[columnDef.name]
                            ?: throw IllegalArgumentException("Column $columnDef does not exist on entity ${this@Entity.name}.")
                    (this.context.getTx(column) as ColumnTx<Value>).update(record.tupleId, value)
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
                this@Entity.columns.values.forEach {
                    (this.context.getTx(it) as ColumnTx<*>).delete(tupleId)
                }

                /* Update header. */
                val header = this@Entity.header
                header.size -= 1
                header.modified = System.currentTimeMillis()
                this@Entity.store.update(HEADER_RECORD_ID, header, EntityHeaderSerializer)
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Deleting record $tupleId failed due to an error in the underlying storage: ${e.message}.")
            }
        }

        /**
         * Performs a COMMIT of all changes made through this [Entity.Tx].
         */
        override fun performCommit() {
            this@Entity.store.commit()
        }

        /**
         * Performs a ROLLBACK of all changes made through this [Entity.Tx].
         */
        override fun performRollback() {
            this@Entity.store.rollback()
        }

        /**
         * Closes all the [ColumnTx] and [IndexTx] and releases the [closeLock] on the [Entity].
         */
        override fun cleanup() {
            this@Entity.closeLock.unlockRead(this.closeStamp)
        }
    }
}
