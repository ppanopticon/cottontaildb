package org.vitrivr.cottontail.database.index

import org.mapdb.DB
import org.mapdb.DBException
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.database.general.TxAction
import org.vitrivr.cottontail.database.general.TxSnapshot
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.utilities.extensions.write
import org.vitrivr.cottontail.utilities.io.TxFileUtilities

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.StampedLock

/**
 * An abstract [Index] implementation that outlines the fundamental structure. Implementations of
 * [Index]es in Cottontail DB should inherit from this class.
 *
 * @see Index
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
abstract class AbstractIndex(final override val path: Path, final override val parent: Entity) : Index {
    /**
     * Companion object of the [DefaultEntity]
     */
    companion object {

        /** Field name for the [IndexHeader] entry.  */
        const val INDEX_HEADER_FIELD = "cdb_index_header"

        /** Field name for the index configuration entries.  */
        const val INDEX_CONFIG_FIELD = "cdb_index_config"

        /** Field name for the [AbstractIndex]es 'dirty' entry.  */
        const val INDEX_DIRTY_FIELD = "cdb_index_dirty"

        /**
         * Initializes a new Cottontail DB [AbstractIndex].
         *
         * @param path [Path] to the index.
         * @param name The [Name] of the index.
         * @param type The [IndexType] of the [IndexHeader]
         * @param columns The [ColumnDef] indexed by the [IndexHeader]
         * @param config The Cottontail DB  configuration
         */
        fun initialize(path: Path, name: Name.IndexName, type: IndexType, columns: Array<ColumnDef<*>>, config: Config) {
            val dataPath = TxFileUtilities.createPath(path.resolve("${name.simple}.idx"))
            if (Files.exists(path)) throw DatabaseException.InvalidFileException("Failed to create index '$name': A file already exists under $path.")
            try {
                val db: DB = config.mapdb.db(path)
                val header = db.atomicVar(INDEX_HEADER_FIELD, IndexHeader.Serializer).create()
                header.set(IndexHeader(name.simple, type, columns))
                db.commit()
                db.close()
            } catch (e: DBException) {
                TxFileUtilities.delete(dataPath) /* Cleanup. */
                throw DatabaseException("Failed to create index '$name' due to error in the underlying data store: {${e.message}")
            } catch (e: IOException) {
                TxFileUtilities.delete(dataPath) /* Cleanup. */
                throw DatabaseException("Failed to create index '$name' due to an IO exception: {${e.message}")
            } catch (e: Throwable) {
                TxFileUtilities.delete(dataPath) /* Cleanup. */
                throw DatabaseException("Failed to create index '$name' due to an unexpected error: {${e.message}")
            }
        }
    }

    /** An internal lock that is used to synchronize structural changes to an [AbstractIndex] (e.g. closing or deleting) with running [AbstractIndex.Tx]. */
    protected val closeLock = StampedLock()

    /** The internal [DB] reference for this [AbstractIndex]. */
    protected val store: DB = this.parent.parent.parent.config.mapdb.db(this.path)

    /** The [IndexHeader] for this [AbstractIndex]. */
    private val headerField = this.store.atomicVar(INDEX_HEADER_FIELD, IndexHeader.Serializer).createOrOpen()

    /** Internal storage variable for the dirty flag. */
    protected val dirtyField = this.store.atomicBoolean(INDEX_DIRTY_FIELD).createOrOpen()

    /** The [Name.IndexName] of this [AbstractIndex]. */
    override val name: Name.IndexName = this.parent.name.index(this.headerField.get().name)

    /** The [ColumnDef] that are covered (i.e. indexed) by this [AbstractIndex]. */
    override val columns: Array<ColumnDef<*>> = this.headerField.get().columns

    /** The order in which results of this [Index] appear. Defaults to an empty array, which indicates no particular order. */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>> = emptyArray()

    /** The [DBOVersion] of this [AbstractIndex]. */
    override val version: DBOVersion = DBOVersion.V2_0

    /** The [IndexConfig] for this [AbstractIndex]. Defaults to [NoIndexConfig]. */
    override val config: IndexConfig = NoIndexConfig

    /** Flag indicating, if this [AbstractIndex] reflects all changes done to the [DefaultEntity]it belongs to. */
    override val dirty: Boolean
        get() = this.dirtyField.get()

    /** Flag indicating if this [AbstractIndex] has been closed. */
    override val closed: Boolean
        get() = this.store.isClosed()

    /** Closes this [AbstractIndex] and the associated data structures. */
    override fun close() = this.closeLock.write {
        if (!this.closed) {
            this.store.close()
        }
    }

    /**
     * A [Tx] that affects this [AbstractIndex].
     */
    protected abstract inner class Tx(context: TransactionContext) : AbstractTx(context), IndexTx {

        /** Obtains a global (non-exclusive) read-lock on [AbstractIndex]. Prevents enclosing [AbstractIndex] from being closed. */
        private val closeStamp = this@AbstractIndex.closeLock.readLock()

        /** Reference to the [AbstractIndex] */
        override val dbo: AbstractIndex
            get() = this@AbstractIndex

        /** The order in which results of this [IndexTx] appear. Empty array that there is no particular order. */
        override val order: Array<Pair<ColumnDef<*>, SortOrder>>
            get() = this@AbstractIndex.order

        /** The [IndexType] of the [AbstractIndex] that underpins this [IndexTx]. */
        override val type: IndexType
            get() = this@AbstractIndex.type

        /** The default [TxSnapshot] of this [IndexTx]. Can be overridden! */
        override val snapshot by lazy {
            object : TxSnapshot {
                override val actions: List<TxAction> = emptyList()
                override fun commit() = this@AbstractIndex.store.commit()
                override fun rollback() = this@AbstractIndex.store.rollback()
                override fun record(action: TxAction): Boolean = false
            }
        }

        /**
         * Checks if this [IndexTx] can process the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return True if [Predicate] can be processed, false otherwise.
         */
        override fun canProcess(predicate: Predicate): Boolean = this@AbstractIndex.canProcess(predicate)

        /**
         * Releases the [closeLock] in the [AbstractIndex].
         */
        override fun cleanup() {
            this@AbstractIndex.closeLock.unlock(this.closeStamp)
        }
    }
}
