package org.vitrivr.cottontail.database.catalogue

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap

import org.mapdb.*
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.TxStatus
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.database.schema.SchemaHeader
import org.vitrivr.cottontail.database.schema.SchemaHeaderSerializer
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.*
import java.util.concurrent.locks.StampedLock
import java.util.stream.Collectors


/**
 * The main catalogue in Cottontail DB. It contains references to all the [Schema]s managed by Cottontail
 * and is the main way of accessing these [Schema]s and creating new ones.
 *
 * @see Schema
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class Catalogue(val config: Config) : DBO {
    /**
     * Companion object to [Catalogue]
     */
    companion object {
        /** ID of the schema header! */
        internal const val HEADER_RECORD_ID: Long = 1L

        /** Filename for the [Entity] catalogue.  */
        internal const val FILE_CATALOGUE = "catalogue.db"
    }

    /** Root to Cottontail DB root folder. */
    override val path: Path = config.root

    /** Constant name of the [Catalogue] object. */
    override val name: Name.RootName = Name.RootName

    /** Constant parent [DBO], which is null in case of the [Catalogue]. */
    override val parent: DBO? = null

    /** A lock used to mediate access to this [Catalogue]. */
    private val closeLock = StampedLock()

    /** The [StoreWAL] that contains the Cottontail DB catalogue. */
    private val store: CottontailStoreWAL = path.let {
        val file = this.path.resolve(FILE_CATALOGUE)
        if (Files.exists(file)) {
            openStore(file)
        } else {
            initStore(file)
        }
    }

    /** Reference to the [CatalogueHeader] of the [Catalogue]. Accessing it will read right from the underlying store. */
    private val header: CatalogueHeader
        get() = this.store.get(HEADER_RECORD_ID, CatalogueHeaderSerializer)
                ?: throw DatabaseException.DataCorruptionException("Failed to open Cottontail DB catalogue header!")

    /** A in-memory registry of all the [Schema]s contained in this [Catalogue]. When a [Catalogue] is opened, all the [Schema]s will be loaded. */
    private val registry: MutableMap<Name.SchemaName, Schema> = Collections.synchronizedMap(Object2ObjectOpenHashMap<Name.SchemaName, Schema>())

    /** Size of this [Catalogue] in terms of [Schema]s it contains. */
    val size: Int
        get() = this.closeLock.read { this.header.schemas.size }

    /** Status indicating whether this [Catalogue] is open or closed. */
    @Volatile
    override var closed: Boolean = false
        private set

    /** Initialization logic for [Catalogue]. */
    init {
        val header = this.header
        for (sid in header.schemas) {
            val schema = this.store.get(sid, CatalogueEntrySerializer)
                    ?: throw DatabaseException.DataCorruptionException("Failed to open Cottontail DB catalogue entry!")
            val path = this.path.resolve("schema_${schema.name}")
            if (!Files.exists(path)) {
                throw DatabaseException.DataCorruptionException("Broken catalogue entry for schema '${schema.name}'. Path $path does not exist!")
            }
            val s = Schema(Name.SchemaName(schema.name), this)
            this.registry[s.name] = s
        }
    }


    /**
     * Opens the data store underlying this Cottontail DB [Catalogue]
     *
     * @param path The path to the data store file.
     * @return [StoreWAL] object.
     */
    private fun openStore(path: Path): CottontailStoreWAL = try {
        this.config.mapdb.store(path)
    } catch (e: DBException) {
        throw DatabaseException("Failed to open Cottontail DB catalogue: ${e.message}'.")
    }

    /**
     * Initializes a new Cottontail DB [Catalogue] under the given path.
     *
     * @param path The path to the data store file.
     * @return [StoreWAL] object.
     */
    private fun initStore(path: Path) = try {
        try {
            if (!Files.exists(path.parent)) {
                Files.createDirectories(path.parent)
            }
        } catch (e: IOException) {
            throw DatabaseException("Failed to create Cottontail DB catalogue due to an IO exception: ${e.message}")
        }

        /* Create and initialize new store. */
        val store = this.config.mapdb.store(this.config.root.resolve(FILE_CATALOGUE))
        store.put(CatalogueHeader(), CatalogueHeaderSerializer)
        store.commit()
        store
    } catch (e: DBException) {
        throw DatabaseException("Failed to initialize the Cottontail DB catalogue: ${e.message}'.")
    }

    /**
     * Creates and returns a new [Catalogue.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [Catalogue.Tx] for.
     * @return New [Catalogue.Tx]
     */
    override fun newTx(context: TransactionContext): Tx = Tx(context)

    /**
     * Closes the [Catalogue] and all objects contained within.
     */
    override fun close() = this.closeLock.write {
        this.registry.forEach { (_, v) -> v.close() }
        this.store.close()
        this.closed = true
    }

    /**
     * A [Tx] that affects this [Catalogue].
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    inner class Tx(context: TransactionContext) : AbstractTx(context), CatalogueTx {

        /** Reference to the [Catalogue] this [CatalogueTx] belongs to. */
        override val dbo: Catalogue
            get() = this@Catalogue

        /** Obtains a global (non-exclusive) read-lock on [Catalogue]. Prevents enclosing [Schema] from being closed. */
        private val closeStamp = this@Catalogue.closeLock.readLock()

        /** Actions that should be executed after committing this [Tx]. */
        private val postCommitAction = mutableListOf<Runnable>()

        /** Actions that should be executed after rolling back this [Tx]. */
        private val postRollbackAction = mutableListOf<Runnable>()

        /**
         * Returns a list of [Name.SchemaName] held by this [Catalogue].
         *
         * @return [List] of all [Name.SchemaName].
         */
        override fun listSchemas(): List<Name.SchemaName> = this.withReadLock {
            this@Catalogue.header.schemas.map {
                Name.SchemaName(this@Catalogue.store.get(it, Serializer.STRING)
                        ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue ($path): Could not find schema name of RecId $it."))
            }
        }

        /**
         * Returns the [Schema] for the given [Name.SchemaName].
         *
         * @param name [Name.SchemaName] to obtain the [Schema] for.
         */
        override fun schemaForName(name: Name.SchemaName): Schema = this.withReadLock {
            this@Catalogue.registry[name]
                    ?: throw DatabaseException.SchemaDoesNotExistException(name)
        }

        /**
         * Creates a new, empty [Schema] with the given [Name.SchemaName] and [Path]
         *
         * @param name The [Name.SchemaName] of the new [Schema].
         */
        override fun createSchema(name: Name.SchemaName): Schema = this.withWriteLock {
            /* Check if schema with that name exists. */
            if (this.listSchemas()
                    .contains(name)
            ) throw DatabaseException.SchemaAlreadyExistsException(name)

            try {
                /* Create empty folder for entity. */
                val path = this@Catalogue.path.resolve("schema_${name.simple}")
                if (!Files.exists(path)) {
                    Files.createDirectories(path)
                } else {
                    throw DatabaseException("Failed to create schema '$name'. Data directory '$path' seems to be occupied.")
                }

                /* Generate the store for the new schema. */
                val store = this@Catalogue.config.mapdb.store(path.resolve(Schema.FILE_CATALOGUE))
                store.put(SchemaHeader(), SchemaHeaderSerializer)
                store.commit()
                store.close()

                /* Update catalogue. */
                val sid =
                    this@Catalogue.store.put(CatalogueEntry(name.simple), CatalogueEntrySerializer)

                /* Update header. */
                val new = this@Catalogue.header.let {
                    CatalogueHeader(
                        it.size + 1,
                        it.created,
                        System.currentTimeMillis(),
                        it.schemas.copyOf(it.schemas.size + 1)
                    )
                }
                new.schemas[new.schemas.size - 1] = sid
                this@Catalogue.store.update(HEADER_RECORD_ID, new, CatalogueHeaderSerializer)

                /* ON COMMIT: Make schema available. */
                val schema = Schema(name, this@Catalogue)
                this.postCommitAction.add {
                    this@Catalogue.registry[name] = schema
                }

                /* ON ROLLBACK: Remove schema folder. */
                this.postRollbackAction.add {
                    schema.close()
                    val pathsToDelete = Files.walk(path).sorted(Comparator.reverseOrder())
                        .collect(Collectors.toList())
                    pathsToDelete.forEach { Files.delete(it) }
                }

                return schema
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Failed to create schema '$name' due to a storage exception: ${e.message}")
            } catch (e: IOException) {
                throw DatabaseException("Failed to create schema '$name' due to an IO exception: ${e.message}")
            }
        }

        /**
         * Drops an existing [Schema] with the given [Name.SchemaName].
         *
         * @param name The [Name.SchemaName] of the [Schema] to be dropped.
         */
        override fun dropSchema(name: Name.SchemaName) = this.withWriteLock {
            /* Obtain schema and acquire exclusive lock on it. */
            val schema = this.schemaForName(name)
            if (this.context.lockOn(schema) == LockMode.NO_LOCK) {
                this.context.requestLock(schema, LockMode.EXCLUSIVE)
            }

            /* Close schema and remove from registry. This is a reversible operation! */
            schema.close()
            this@Catalogue.registry.remove(name)

            /* Remove catalogue entry + update header. */
            try {
                /* Rename folder and mark it for deletion. */
                val shadowSchema = schema.path.resolveSibling(schema.path.fileName.toString() + "~dropped")
                Files.move(schema.path, shadowSchema, StandardCopyOption.ATOMIC_MOVE)

                /* ON ROLLBACK: Move back schema data and re-open it. */
                this.postRollbackAction.add {
                    Files.move(shadowSchema, schema.path)
                    this@Catalogue.registry[name] = Schema(name, this@Catalogue)
                    this.context.releaseLock(schema)
                }

                /* ON COMMIT: Remove schema from registry and delete files. */
                this.postCommitAction.add {
                    val pathsToDelete = Files.walk(shadowSchema).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
                    pathsToDelete.forEach { Files.deleteIfExists(it) }
                    this.context.releaseLock(schema)
                }

                /* Extract the catalogue entry. */
                val catalogueEntry = this@Catalogue.header.schemas.map {
                    it to (this@Catalogue.store.get(it, CatalogueEntrySerializer)
                            ?: throw DatabaseException.DataCorruptionException("Failed to read Cottontail DB catalogue entry for RecId $it!"))
                }.find { it.second.name == name.simple }
                        ?: throw DatabaseException.DataCorruptionException("Failed to drop schema '$name'. Did not find a Cottontail DB catalogue entry for schema $name!")

                this@Catalogue.store.delete(catalogueEntry.first, CatalogueEntrySerializer)
                val new = this@Catalogue.header.let { CatalogueHeader(it.size - 1, it.created, System.currentTimeMillis(), it.schemas.filter { it != catalogueEntry.first }.toLongArray()) }
                this@Catalogue.store.update(HEADER_RECORD_ID, new, CatalogueHeaderSerializer)
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Failed to drop schema '$name' due to a storage exception: ${e.message}")
            } catch (e: IOException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Failed to drop schema '$name' due to a IO exception: ${e.message}")
            }
        }

        /**
         * Performs a commit of all changes made through this [Catalogue.Tx].
         */
        override fun performCommit() {
            /* Perform commit. */
            this@Catalogue.store.commit()

            /* Execute post-commit actions. */
            this.postCommitAction.forEach { it.run() }
            this.postRollbackAction.clear()
            this.postCommitAction.clear()
        }

        /**
         * Performs a rollback of all changes made through this [Catalogue.Tx].
         */
        override fun performRollback() {
            /* Perform rollback. */
            this@Catalogue.store.rollback()

            /* Execute post-rollback actions. */
            this.postRollbackAction.forEach { it.run() }
            this.postRollbackAction.clear()
            this.postCommitAction.clear()
        }

        /**
         * Releases the [closeLock] on the [Catalogue].
         */
        override fun cleanup() {
            this@Catalogue.closeLock.unlockRead(this.closeStamp)
        }
    }
}