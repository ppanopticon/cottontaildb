package org.vitrivr.cottontail.database.schema

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.mapdb.DB
import org.mapdb.DBException
import org.mapdb.Store
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnEngine
import org.vitrivr.cottontail.database.column.mapdb.MapDBColumn
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.*
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.TxException
import org.vitrivr.cottontail.utilities.extensions.write
import org.vitrivr.cottontail.utilities.io.TxFileUtilities
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.*
import java.util.concurrent.locks.StampedLock

/**
 * Default [Schema] implementation in Cottontail DB based on Map DB.
 *
 * @see Schema
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class DefaultSchema(override val path: Path, override val parent: Catalogue) : Schema {
    /** Companion object with different constants. */
    companion object {
        /** Filename for the [DefaultEntity] catalogue.  */
        private const val SCHEMA_HEADER_FIELD = "cdb_entity_header"

        /** Filename for the [DefaultSchema] catalogue.  */
        private const val FILE_CATALOGUE = "index.db"

        /**
         * Initializes an empty [DefaultSchema] on disk.
         *
         * @param name The [Name.SchemaName] of the [DefaultSchema]
         * @param path The [Path] under which to create the [DefaultSchema]
         * @param config The [Config] for to use for creation.
         */
        fun initialize(name: Name.SchemaName, path: Path, config: Config): Path {
            val dataPath = TxFileUtilities.createPath(path.resolve("schema_${name.simple}"))
            if (Files.exists(dataPath)) throw DatabaseException.InvalidFileException("Failed to create schema '$name'. Data directory '$dataPath' seems to be occupied.")
            Files.createDirectories(dataPath)

            /* Generate the store for the new schema. */
            try {
                config.mapdb.db(dataPath.resolve(FILE_CATALOGUE)).use { store ->
                    val schemaHeader = store.atomicVar(SCHEMA_HEADER_FIELD, SchemaHeader.Serializer).create()
                    schemaHeader.set(SchemaHeader(name.simple))
                    store.commit()
                }
                return dataPath
            } catch (e: DBException) {
                TxFileUtilities.delete(dataPath) /* Cleanup. */
                throw DatabaseException("Failed to create schema '$name' due to error in the underlying data store: {${e.message}")
            } catch (e: IOException) {
                TxFileUtilities.delete(dataPath) /* Cleanup. */
                throw DatabaseException("Failed to create schema '$name' due to an IO exception: {${e.message}")
            } catch (e: Throwable) {
                TxFileUtilities.delete(dataPath) /* Cleanup. */
                throw DatabaseException("Failed to create schema '$name' due to an unexpected error: {${e.message}")
            }
        }
    }

    /** Internal reference to the [Store] underpinning this [MapDBColumn]. */
    private val store: DB = try {
        this.parent.config.mapdb.db(this.path.resolve(FILE_CATALOGUE))
    } catch (e: DBException) {
        throw DatabaseException("Failed to open schema at '$path': ${e.message}'")
    }

    /** The [SchemaHeader] of this [DefaultSchema]. */
    private val headerField = this.store.atomicVar(SCHEMA_HEADER_FIELD, SchemaHeader.Serializer).createOrOpen()

    /** A lock used to mediate access the closed state of this [DefaultSchema]. */
    private val closeLock = StampedLock()

    /** A map of loaded [DefaultEntity] references. */
    private val registry: MutableMap<Name.EntityName, Entity> = Collections.synchronizedMap(Object2ObjectOpenHashMap())

    /** The [Name.SchemaName] of this [DefaultSchema]. */
    override val name: Name.SchemaName = Name.SchemaName(this.headerField.get().name)

    /** The [DBOVersion] of this [DefaultSchema]. */
    override val version: DBOVersion
        get() = DBOVersion.V2_0

    /** Flag indicating whether or not this [DefaultSchema] has been closed. */
    override val closed: Boolean
        get() = this.store.isClosed()

    init {
        /* Initialize all entities. */
        this.headerField.get().entities.map {
            val path = this.path.resolve("entity_${it.name}")
            this.registry[this.name.entity(it.name)] = DefaultEntity(path, this)
        }
    }

    /**
     * Creates and returns a new [DefaultSchema.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [DefaultSchema.Tx] for.
     * @return New [DefaultSchema.Tx]
     */
    override fun newTx(context: TransactionContext) = this.Tx(context)

    /**
     * Closes this [DefaultSchema] and all the [DefaultEntity] objects that are contained within.
     *
     * Since locks to [DBO] or [Transaction][org.vitrivr.cottontail.database.general.Tx]
     * objects may be held by other threads, it can take a
     * while for this method to complete.
     */
    override fun close() = this.closeLock.write {
        if (!this.closed) {
            this.store.close()
            this.registry.entries.removeIf {
                it.value.close()
                true
            }
        }
    }

    /**
     * A [Tx] that affects this [DefaultSchema].
     *
     * @author Ralph Gasser
     * @version 2.0.0
     */
    inner class Tx(context: TransactionContext) : AbstractTx(context), SchemaTx {

        /** Obtains a global (non-exclusive) read-lock on [DefaultSchema]. Prevents enclosing [DefaultSchema] from being closed. */
        private val closeStamp = this@DefaultSchema.closeLock.readLock()

        /** Reference to the surrounding [DefaultSchema]. */
        override val dbo: DBO
            get() = this@DefaultSchema

        /**
         * The [SchemaTxSnapshot] of this [SchemaTx].
         *
         * Important: The [SchemaTxSnapshot] is created lazily upon first access, which means that whatever
         * caller creates it, it holds the necessary locks!
         */
        override val snapshot by lazy {
            object : SchemaTxSnapshot {
                /** List of entities available to this [Tx]. */
                override val entities = Object2ObjectOpenHashMap(this@DefaultSchema.registry)

                /** A map of all [TxAction] executed by this [SchemaTx]. Can be seen as an in-memory WAL. */
                override val actions = LinkedList<TxAction>()

                /**
                 * Commits the [SchemaTx] and integrates all changes made through it into the [DefaultSchema].
                 */
                override fun commit() {
                    try {
                        /* Materialize changes in surrounding schema (in-memory). */
                        this.actions.forEach {
                            it.commit()
                        }

                        /* Update update header and commit changes. */
                        val newHeader = this@DefaultSchema.headerField.get().copy(
                            modified = System.currentTimeMillis(),
                            entities = this.entities.map { SchemaHeader.EntityRef(it.value.name.simple) }
                        )
                        this@DefaultSchema.headerField.set(newHeader)
                        this@DefaultSchema.store.commit()
                    } catch (e: Throwable) {
                        this@Tx.status = TxStatus.ERROR
                        this@DefaultSchema.store.rollback()
                        throw DatabaseException("Failed to commit schema ${this@DefaultSchema.name} due to an exception: ${e.message}", e)
                    }
                }

                /**
                 * Rolls back this [SchemaTx] and reverts all changes made through it.
                 */
                override fun rollback() {
                    this.actions.forEach { it.rollback() }
                    this@DefaultSchema.store.rollback()
                }

                /**
                 * Records a [TxAction] with this [TxSnapshot].
                 *
                 * @param action The [TxAction] to record.
                 * @return True on success, false otherwise.
                 */
                override fun record(action: TxAction): Boolean = when (action) {
                    is CreateEntityTxAction,
                    is DropEntityTxAction -> {
                        this.actions.add(action)
                        true
                    }
                    else -> false
                }
            }
        }

        /** Checks if DBO is still open. */
        init {
            if (this@DefaultSchema.closed) {
                this@DefaultSchema.closeLock.unlockRead(this.closeStamp)
                throw TxException.TxDBOClosedException(this.context.txId, this@DefaultSchema)
            }
        }

        /**
         * Returns a list of [DefaultEntity] held by this [DefaultSchema].
         *
         * @return [List] of all [Name.EntityName].
         */
        override fun listEntities(): List<Entity> = this.withReadLock {
            this.snapshot.entities.values.toList()
        }

        /**
         * Returns an instance of [DefaultEntity] if such an instance exists. If the [DefaultEntity] has been loaded before,
         * that [DefaultEntity] is re-used. Otherwise, the [DefaultEntity] will be loaded from disk.
         *
         * @param name Name of the [DefaultEntity] to access.
         * @return [DefaultEntity] or null.
         */
        override fun entityForName(name: Name.EntityName): Entity = this.withReadLock {
            this.snapshot.entities[name] ?: throw DatabaseException.EntityDoesNotExistException(name)
        }

        /**
         * Creates a new [DefaultEntity] in this [DefaultSchema].
         *
         * @param name The name of the [DefaultEntity] that should be created.
         * @param columns The [ColumnDef] of the columns the new [DefaultEntity] should have
         */
        override fun createEntity(name: Name.EntityName, vararg columns: Pair<ColumnDef<*>, ColumnEngine>): Entity = this.withWriteLock {
            /* Perform some sanity checks. */
            if (this.snapshot.entities.contains(name)) throw DatabaseException.EntityAlreadyExistsException(name)
            val distinctSize = columns.map { it.first.name }.distinct().size
            if (distinctSize != columns.size) {
                val cols = columns.map { it.first.name }
                throw DatabaseException.DuplicateColumnException(name, cols)
            }

            /* Initialize entity on disk and make it available to transaction. */
            try {
                val data = DefaultEntity.initialize(name, this@DefaultSchema.path, this@DefaultSchema.parent.config, columns.toList())
                val entity = DefaultEntity(data, this@DefaultSchema)
                this.snapshot.record(CreateEntityTxAction(entity))
                this.snapshot.entities[name] = entity
                return entity
            } catch (e: DatabaseException) {
                this.status = TxStatus.ERROR
                throw e
            }
        }

        /**
         * Drops an [DefaultEntity] from this [DefaultSchema].
         *
         * @param name The name of the [DefaultEntity] that should be dropped.
         */
        override fun dropEntity(name: Name.EntityName) = this.withWriteLock {
            /* Get entity and try to obtain lock. */
            val entity = this.snapshot.entities[name] ?: throw DatabaseException.EntityDoesNotExistException(name)
            this.context.requestLock(entity, LockMode.EXCLUSIVE)

            /* Remove entity from local snapshot. */
            this.snapshot.record(DropEntityTxAction(name))
            this.snapshot.entities.remove(name)
            Unit
        }

        /**
         * Releases the [closeLock] on the [DefaultSchema].
         */
        override fun cleanup() {
            this@DefaultSchema.closeLock.unlockRead(this.closeStamp)
        }

        /**
         * A [TxAction] for creating a new [Entity].
         *
         * @param entity [Entity] that has been created.
         */
        inner class CreateEntityTxAction(private val entity: Entity) : TxAction {
            override fun commit() {
                this.entity.close()
                val move = Files.move(this.entity.path, TxFileUtilities.plainPath(this.entity.path), StandardCopyOption.ATOMIC_MOVE)
                this@DefaultSchema.registry[this.entity.name] = DefaultEntity(move, this.entity.parent)
            }

            override fun rollback() {
                this.entity.close()
                TxFileUtilities.delete(this.entity.path)
            }
        }

        /**
         * A [TxAction] implementation for dropping an [Entity].
         *
         * @param entity [Entity] that has been dropped.
         */
        inner class DropEntityTxAction(private val entity: Name.EntityName) : TxAction {
            override fun commit() {
                val entity = this@DefaultSchema.registry.remove(this.entity) ?: throw IllegalStateException("Failed to drop schema $entity because it is unknown to the schema. This is a programmer's error!")
                entity.close()
                if (Files.exists(entity.path)) {
                    TxFileUtilities.delete(entity.path)
                }
            }

            override fun rollback() {}
        }
    }
}



