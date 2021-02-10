package org.vitrivr.cottontail.database.schema

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.mapdb.CottontailStoreWAL
import org.mapdb.DBException
import org.mapdb.Serializer
import org.mapdb.Store
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.mapdb.MapDBColumn
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityHeader
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.TxStatus
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.utilities.extensions.read
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.*
import java.util.concurrent.locks.StampedLock
import java.util.stream.Collectors

/**
 * Represents an schema in the Cottontail DB data model. A [Schema] is a collection of [Entity] objects that belong together
 * (e.g., because they belong to the same application). Every [Schema] can be seen as a dedicated database and different
 * [Schema]s in Cottontail can reside in different locations.
 *
 * Calling the default constructor for [Schema] opens that [Schema]. It can only be opened once due to file locks and it
 * will remain open until the [Schema.close()] method is called.
 *
 * @see Entity
 * @see MapDBColumn
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class Schema(override val name: Name.SchemaName, override val parent: Catalogue) : DBO {
    /**
     * Companion object with different constants.
     */
    companion object {
        /** ID of the schema header! */
        const val HEADER_RECORD_ID: Long = 1L

        /** Filename for the [Schema] catalogue.  */
        const val FILE_CATALOGUE = "index.db"
    }

    /** The [Path] to the [Schema]'s main folder. */
    override val path = this.parent.path.resolve("schema_${name.simple}")

    /** Internal reference to the [Store] underpinning this [MapDBColumn]. */
    private val store: CottontailStoreWAL = try {
        this.parent.config.mapdb.store(this.path.resolve(FILE_CATALOGUE))
    } catch (e: DBException) {
        throw DatabaseException("Failed to open schema $name at '$path': ${e.message}'")
    }

    /** Reference to the [SchemaHeader] of the [Schema]. */
    private val header
        get() = this.store.get(HEADER_RECORD_ID, SchemaHeaderSerializer)
                ?: throw DatabaseException.DataCorruptionException("Failed to open header of schema $name!")

    /** A lock used to mediate access the closed state of this [Schema]. */
    private val closeLock = StampedLock()

    /** A map of loaded [Entity] references. */
    private val registry: MutableMap<Name.EntityName, Entity> = Collections.synchronizedMap(Object2ObjectOpenHashMap())

    /** Flag indicating whether or not this [Schema] has been closed. */
    @Volatile
    override var closed: Boolean = false
        private set

    init {
        /* Initialize all entities. */
        this.header.entities.map {
            val name = this.name.entity(this.store.get(it, Serializer.STRING)
                    ?: throw DatabaseException.DataCorruptionException("Failed to read schema $name ($path): Could not find entity name of ID $it."))
            this.registry[name] = Entity(name, this)
        }
    }

    /**
     * Creates and returns a new [Schema.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [Schema.Tx] for.
     * @return New [Schema.Tx]
     */
    override fun newTx(context: TransactionContext) = this.Tx(context)

    /**
     * Closes this [Schema] and all the [Entity] objects that are contained within.
     *
     * Since locks to [DBO] or [Transaction][org.vitrivr.cottontail.database.general.Tx]
     * objects may be held by other threads, it can take a
     * while for this method to complete.
     */
    override fun close() = this.closeLock.read {
        if (!this.closed) {
            this.registry.entries.removeIf {
                it.value.close()
                true
            }
            this.store.close()
            this.closed = true
        }
    }

    /**
     * A [Tx] that affects this [Schema].
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    inner class Tx(context: TransactionContext) : AbstractTx(context), SchemaTx {

        /** Obtains a global (non-exclusive) read-lock on [Schema]. Prevents enclosing [Schema] from being closed. */
        private val closeStamp = this@Schema.closeLock.readLock()

        /** Reference to the surrounding [Schema]. */
        override val dbo: DBO
            get() = this@Schema

        /** Actions that should be executed after committing this [Tx]. */
        private val postCommitAction = mutableListOf<Runnable>()

        /** Actions that should be executed after rolling back this [Tx]. */
        private val postRollbackAction = mutableListOf<Runnable>()

        /**
         * Returns a list of [Entity] held by this [Schema].
         *
         * @return [List] of all [Name.EntityName].
         */
        override fun listEntities(): List<Name.EntityName> = this.withReadLock {
            return this@Schema.header.entities.map {
                this@Schema.name.entity(this@Schema.store.get(it, Serializer.STRING)
                        ?: throw DatabaseException.DataCorruptionException("Failed to read schema $name ($path): Could not find entity name of ID $it."))
            }
        }

        /**
         * Returns an instance of [Entity] if such an instance exists. If the [Entity] has been loaded before,
         * that [Entity] is re-used. Otherwise, the [Entity] will be loaded from disk.
         *
         * @param name Name of the [Entity] to access.
         * @return [Entity] or null.
         */
        override fun entityForName(name: Name.EntityName): Entity = this.withReadLock {
            return this@Schema.registry[name]
                    ?: throw DatabaseException.EntityDoesNotExistException(name)
        }

        /**
         * Creates a new [Entity] in this [Schema].
         *
         * @param name The name of the [Entity] that should be created.
         * @param columns The [ColumnDef] of the columns the new [Entity] should have
         */
        override fun createEntity(name: Name.EntityName, vararg columns: ColumnDef<*>): Entity =
            this.withWriteLock {

                if (this.listEntities()
                        .contains(name)
                ) throw DatabaseException.EntityAlreadyExistsException(name)
                columns.forEach { i ->
                    if (columns.count { o -> i == o } > 0) {
                        throw DatabaseException.DuplicateColumnException(i.name)
                    }
                }
                try {
                    /* Create empty folder for entity. */
                    val data = this@Schema.path.resolve("entity_${name.simple}")
                    if (!Files.exists(data)) {
                        Files.createDirectories(data)
                    } else {
                        throw DatabaseException("Failed to create entity '$name'. Data directory '$data' seems to be occupied.")
                    }

                    /* Store entry for new entity. */
                val recId = this@Schema.store.put(name.simple, Serializer.STRING)

                /* Generate the entity. */
                val store = this@Schema.parent.config.mapdb.store(data.resolve(Entity.FILE_CATALOGUE))
                store.preallocate() /* Pre-allocates the header. */

                /* Initialize the entities header. */
                val columnIds = columns.map {
                    MapDBColumn.initialize(it, data, this@Schema.parent.config.mapdb)
                    store.put(it.name.simple, Serializer.STRING)
                }.toLongArray()
                    store.update(
                        Entity.HEADER_RECORD_ID,
                        EntityHeader(columns = columnIds),
                        EntityHeader.Serializer
                    )
                    store.commit()
                    store.close()

                    /* Update schema header. */
                    val header = this@Schema.header
                    header.modified = System.currentTimeMillis()
                    header.entities = header.entities.copyOf(header.entities.size + 1)
                    header.entities[header.entities.size - 1] = recId
                    this@Schema.store.update(HEADER_RECORD_ID, header, SchemaHeaderSerializer)

                    /* ON COMMIT: Make entity available. */
                    val entity = Entity(name, this@Schema)
                    this.postCommitAction.add {
                        this@Schema.registry[name] = entity
                    }

                    /* ON ROLLBACK: Delete unused entity folder. */
                    this.postRollbackAction.add {
                        entity.close()
                        val pathsToDelete = Files.walk(data).sorted(Comparator.reverseOrder())
                            .collect(Collectors.toList())
                        pathsToDelete.forEach { Files.delete(it) }
                    }

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
         * Drops an [Entity] from this [Schema].
         *
         * @param name The name of the [Entity] that should be dropped.
         */
        override fun dropEntity(name: Name.EntityName) = this.withWriteLock {
            /* Get entity and try to obtain lock. */
            val entity = this@Schema.registry[name]
                    ?: throw DatabaseException.EntityDoesNotExistException(name)
            val entityRecId = this@Schema.header.entities.find { this@Schema.store.get(it, Serializer.STRING) == name.simple }
                    ?: throw DatabaseException.DataCorruptionException("Could not find RecId for entity $name.")
            if (this.context.lockOn(entity) == LockMode.NO_LOCK) {
                this.context.requestLock(entity, LockMode.EXCLUSIVE)
            }

            /* Close entity and remove it from registry. */
            entity.close()
            this@Schema.registry.remove(name)

            try {
                /* Rename folder and of entity it for deletion. */
                val shadowEntity = entity.path.resolveSibling(entity.path.fileName.toString() + "~dropped")
                Files.move(entity.path, shadowEntity, StandardCopyOption.ATOMIC_MOVE)

                /* ON COMMIT: Remove schema from registry and delete files. */
                this.postCommitAction.add {
                    val pathsToDelete = Files.walk(shadowEntity).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
                    pathsToDelete.forEach { Files.deleteIfExists(it) }
                    this.context.releaseLock(entity)
                }

                /* ON ROLLBACK: Re-map entity and move back files. */
                this.postRollbackAction.add {
                    Files.move(shadowEntity, entity.path, StandardCopyOption.ATOMIC_MOVE)
                    this@Schema.registry[name] = Entity(name, this@Schema)
                    this.context.releaseLock(entity)
                }

                /* Delete entry for entity. */
                this@Schema.store.delete(entityRecId, Serializer.STRING)

                /* Update header. */
                val header = this@Schema.header
                header.modified = System.currentTimeMillis()
                header.entities = header.entities.filter { it != entityRecId }.toLongArray()
                this@Schema.store.update(HEADER_RECORD_ID, header, SchemaHeaderSerializer)
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Entity '$name' could not be dropped, because of an error in the underlying data store: ${e.message}!")
            }
        }

        /**
         * Performs a COMMIT of all changes made through this [Schema.Tx].
         */
        override fun performCommit() {
            /* Perform commit. */
            this@Schema.store.commit()

            /* Execute post-commit actions. */
            this.postCommitAction.forEach { it.run() }
            this.postRollbackAction.clear()
            this.postCommitAction.clear()
        }

        /**
         * Performs a ROLLBACK of all changes made through this [Schema.Tx].
         */
        override fun performRollback() {
            /* Perform rollback. */
            this@Schema.store.rollback()

            /* Execute post-rollback actions. */
            this.postRollbackAction.forEach { it.run() }
            this.postRollbackAction.clear()
            this.postCommitAction.clear()
        }

        /**
         * Releases the [closeLock] on the [Schema].
         */
        override fun cleanup() {
            this@Schema.closeLock.unlockRead(this.closeStamp)
        }
    }
}



