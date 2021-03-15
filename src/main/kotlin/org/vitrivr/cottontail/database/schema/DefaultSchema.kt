package org.vitrivr.cottontail.database.schema

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.mapdb.*
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnEngine
import org.vitrivr.cottontail.database.column.mapdb.MapDBColumn
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityHeader
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.database.general.TxStatus
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.utilities.io.FileUtilities
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.StampedLock

/**
 * Default [Schema] implementation in Cottontail DB based on Map DB.
 *
 * @see Schema
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class DefaultSchema(override val path: Path, override val parent: DefaultCatalogue) : Schema {
    /** Companion object with different constants. */
    companion object {
        /** Filename for the [DefaultEntity] catalogue.  */
        const val SCHEMA_HEADER_FIELD = "cdb_entity_header"

        /** Filename for the [DefaultSchema] catalogue.  */
        const val FILE_CATALOGUE = "index.db"
    }

    /** Internal reference to the [Store] underpinning this [MapDBColumn]. */
    private val store: DB = try {
        this.parent.config.mapdb.db(this.path.resolve(FILE_CATALOGUE))
    } catch (e: DBException) {
        throw DatabaseException("Failed to open schema at '$path': ${e.message}'")
    }

    /** The [SchemaHeader] of this [DefaultSchema]. */
    private val headerField =
        this.store.atomicVar(SCHEMA_HEADER_FIELD, SchemaHeader.Serializer).createOrOpen()

    /** A lock used to mediate access the closed state of this [DefaultSchema]. */
    private val closeLock = StampedLock()

    /** A map of loaded [DefaultEntity] references. */
    private val registry: MutableMap<Name.EntityName, Entity> =
        Collections.synchronizedMap(Object2ObjectOpenHashMap())

    /** The [Name.SchemaName] of this [DefaultSchema]. */
    override val name: Name.SchemaName = Name.SchemaName(this.headerField.get().name)

    /** The [DBOVersion] of this [DefaultSchema]. */
    override val version: DBOVersion
        get() = DBOVersion.V2_0

    /** Flag indicating whether or not this [DefaultSchema] has been closed. */
    @Volatile
    override var closed: Boolean = false
        private set

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
    override fun close() {
        if (!this.closed) {
            try {
                val stamp = this.closeLock.tryWriteLock(1000, TimeUnit.MILLISECONDS)
                try {
                    this.registry.entries.removeIf {
                        it.value.close()
                        true
                    }
                    this.store.close()
                    this.closed = true
                } catch (e: Throwable) {
                    this.closeLock.unlockWrite(stamp)
                    throw e
                }
            } catch (e: InterruptedException) {
                throw IllegalStateException("Could not close schema ${this.name}. Failed to acquire exclusive lock which indicates, that transaction wasn't closed properly.")
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

        /** The [SchemaTxSnapshot] of this [SchemaTx]. */
        override val snapshot = object : SchemaTxSnapshot {
            /** List of entities available to this [Tx]. */
            override val entities = Object2ObjectOpenHashMap(this@DefaultSchema.registry)

            /**
             * Commits the [SchemaTx] and integrates all changes made through it into the [DefaultSchema].
             */
            override fun commit() {
                /* Update update header and commit changes. */
                try {
                    val newHeader = this@DefaultSchema.headerField.get().copy(
                        modified = System.currentTimeMillis(),
                        entities = this.entities.map { SchemaHeader.EntityRef(it.value.name.simple) }
                    )
                    this@DefaultSchema.headerField.set(newHeader)
                    this@DefaultSchema.store.commit()
                } catch (e: DBException) {
                    this@Tx.status = TxStatus.ERROR
                    this@DefaultSchema.store.rollback()
                    throw DatabaseException("Failed to commit schema ${this@DefaultSchema.name} due to a storage exception: ${e.message}")
                }

                /* Materialize created entities. */
                this.entities.forEach {
                    if (!this@DefaultSchema.registry.contains(it.key)) {
                        this@DefaultSchema.registry[it.key] = it.value
                    }
                }

                /* Materialize dropped entities. */
                val remove = this@DefaultSchema.registry.values.filter {
                    !this.entities.containsKey(it.name)
                }
                remove.forEach {
                    try {
                        it.close()
                        FileUtilities.deleteRecursively(it.path)
                    } finally {
                        this@DefaultSchema.registry.remove(it.name)
                    }
                }
            }

            /* Delete newly created entities and commit store. */
            override fun rollback() {
                this.entities.forEach {
                    if (!this@DefaultSchema.registry.contains(it.key)) {
                        it.value.close()
                        FileUtilities.deleteRecursively(it.value.path)
                    }
                }
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
            this.snapshot.entities[name]
                ?: throw DatabaseException.EntityDoesNotExistException(name)
        }

        /**
         * Creates a new [DefaultEntity] in this [DefaultSchema].
         *
         * @param name The name of the [DefaultEntity] that should be created.
         * @param columns The [ColumnDef] of the columns the new [DefaultEntity] should have
         */
        override fun createEntity(
            name: Name.EntityName,
            vararg columns: Pair<ColumnDef<*>, ColumnEngine>
        ): Entity =
            this.withWriteLock {
                /* Perform some sanity checks. */
                if (this.snapshot.entities.contains(name)) throw DatabaseException.EntityAlreadyExistsException(
                    name
                )
                val distinctSize = columns.map { it.first.name }.distinct().size
                if (distinctSize != columns.size) {
                    val cols = columns.map { it.first.name }
                    throw DatabaseException.DuplicateColumnException(name, cols)
                }

                try {
                    /* Create empty folder for entity. */
                    val data = this@DefaultSchema.path.resolve("entity_${name.simple}")
                    if (!Files.exists(data)) {
                        Files.createDirectories(data)
                    } else {
                        throw DatabaseException("Failed to create entity '$name'. Data directory '$data' seems to be occupied.")
                    }

                    /* Generate the entity and initialize the new entities header. */
                    val store =
                        this@DefaultSchema.parent.config.mapdb.db(data.resolve(DefaultEntity.CATALOGUE_FILE))
                    val columnsRefs = columns.map {
                        val path = data.resolve("${it.first.name.simple}.col")
                        when (it.second) {
                            ColumnEngine.MAPDB -> {
                                MapDBColumn.initialize(
                                    path,
                                    it.first,
                                    this@DefaultSchema.parent.config.mapdb
                                )
                            }
                            ColumnEngine.HARE -> throw UnsupportedOperationException("The column driver ${it.second} is currently not supported.")
                        }
                        EntityHeader.ColumnRef(it.first.name.simple, it.second)
                    }
                    val entityHeader = EntityHeader(name = name.simple, columns = columnsRefs)
                    store.atomicVar(DefaultEntity.ENTITY_HEADER_FIELD, EntityHeader.Serializer)
                        .create().set(entityHeader)
                    store.commit()
                    store.close()

                    /* Make entity available to Tx. */
                    val entity = DefaultEntity(data, this@DefaultSchema)
                    this.snapshot.entities[name] = entity
                    return entity
                } catch (e: DBException) {
                    this.status = TxStatus.ERROR
                    throw DatabaseException("Failed to create entity '$name' due to error in the underlying data store: {${e.message}")
                } catch (e: IOException) {
                    this.status = TxStatus.ERROR
                    throw DatabaseException("Failed to create entity '$name' due to an IO exception: {${e.message}")
                }
            }

        /**
         * Drops an [DefaultEntity] from this [DefaultSchema].
         *
         * @param name The name of the [DefaultEntity] that should be dropped.
         */
        override fun dropEntity(name: Name.EntityName) = this.withWriteLock {
            /* Get entity and try to obtain lock. */
            val entity = this@DefaultSchema.registry[name]
                ?: throw DatabaseException.EntityDoesNotExistException(name)
            if (this.context.lockOn(entity) == LockMode.NO_LOCK) {
                this.context.requestLock(entity, LockMode.EXCLUSIVE)
            }

            /* Close entity and remove it from local. */
            this.snapshot.entities.remove(name)
            Unit
        }

        /**
         * Releases the [closeLock] on the [DefaultSchema].
         */
        override fun cleanup() {
            this@DefaultSchema.closeLock.unlockRead(this.closeStamp)
        }
    }
}



