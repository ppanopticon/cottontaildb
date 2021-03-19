package org.vitrivr.cottontail.database.catalogue

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.mapdb.*
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.database.general.TxStatus
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.database.schema.*
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.io.FileUtilities
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.StampedLock

/**
 * The default [Catalogue] implementation based on Map DB.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class DefaultCatalogue(override val config: Config) : Catalogue {
    /**
     * Companion object to [DefaultCatalogue]
     */
    companion object {
        /** ID of the schema header! */
        internal const val CATALOGUE_HEADER_FIELD: String = "cdb_catalogue_header"

        /** Filename for the [DefaultEntity] catalogue.  */
        internal const val FILE_CATALOGUE = "catalogue.db"
    }

    /** Root to Cottontail DB root folder. */
    override val path: Path = config.root

    /** Constant name of the [DefaultCatalogue] object. */
    override val name: Name.RootName
        get() = Name.RootName

    /** The [DBOVersion] of this [DefaultCatalogue]. */
    override val version: DBOVersion
        get() = DBOVersion.V2_0

    /** Constant parent [DBO], which is null in case of the [DefaultCatalogue]. */
    override val parent: DBO? = null

    /** A lock used to mediate access to this [DefaultCatalogue]. */
    private val closeLock = StampedLock()

    /** The [StoreWAL] that contains the Cottontail DB catalogue. */
    private val store: DB = this.config.mapdb.db(this.path.resolve(FILE_CATALOGUE))

    /** Reference to the [CatalogueHeader] of the [DefaultCatalogue]. Accessing it will read right from the underlying store. */
    private val headerField =
        this.store.atomicVar(CATALOGUE_HEADER_FIELD, CatalogueHeader.Serializer).createOrOpen()

    /** A in-memory registry of all the [Schema]s contained in this [DefaultCatalogue]. When a [Catalogue] is opened, all the [Schema]s will be loaded. */
    private val registry: MutableMap<Name.SchemaName, Schema> =
        Collections.synchronizedMap(Object2ObjectOpenHashMap())

    /** Size of this [DefaultCatalogue] in terms of [Schema]s it contains. This is a snapshot and may change anytime! */
    override val size: Int
        get() = this.closeLock.read { this.headerField.get().schemas.size }

    /** Status indicating whether this [DefaultCatalogue] is open or closed. */
    @Volatile
    override var closed: Boolean = false
        private set

    init {
        /* Initialize empty catalogue */
        if (this.headerField.get() == null) {
            this.headerField.set(CatalogueHeader())
            this.store.commit()
        }

        /* Initialize. */
        for (schemaRef in this.headerField.get().schemas) {
            if (schemaRef.path != null && Files.exists(schemaRef.path)) {
                this.registry[Name.SchemaName(schemaRef.name)] = DefaultSchema(path, this)
            } else {
                val path = this.path.resolve("schema_${schemaRef.name}")
                if (!Files.exists(path)) {
                    throw DatabaseException.DataCorruptionException("Broken catalogue entry for schema '${schemaRef.name}'. Path ${schemaRef.path} does not exist!")
                }
                this.registry[Name.SchemaName(schemaRef.name)] = DefaultSchema(path, this)
            }
        }
    }

    /**
     * Creates and returns a new [DefaultCatalogue.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [DefaultCatalogue.Tx] for.
     * @return New [DefaultCatalogue.Tx]
     */
    override fun newTx(context: TransactionContext): Tx = Tx(context)

    /**
     * Closes the [DefaultCatalogue] and all objects contained within.
     */
    override fun close() {
        try {
            val stamp = this.closeLock.tryWriteLock(1000, TimeUnit.MILLISECONDS)
            try {
                this.registry.forEach { (_, v) -> v.close() }
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

    /**
     * A [Tx] that affects this [DefaultCatalogue].
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    inner class Tx(context: TransactionContext) : AbstractTx(context), CatalogueTx {

        /** Reference to the [DefaultCatalogue] this [CatalogueTx] belongs to. */
        override val dbo: DefaultCatalogue
            get() = this@DefaultCatalogue

        /** Obtains a global (non-exclusive) read-lock on [DefaultCatalogue]. Prevents enclosing [Schema] from being closed. */
        private val closeStamp = this@DefaultCatalogue.closeLock.readLock()

        /** The [CatalogueTxSnapshot] of this [CatalogueTx]. */
        override val snapshot = object : CatalogueTxSnapshot {
            override val schemas = Object2ObjectOpenHashMap(this@DefaultCatalogue.registry)

            /* Make changes to indexes available to entity and persist them. */
            override fun commit() {
                /* Update update header and commit changes. */
                try {
                    val oldHeader = this@DefaultCatalogue.headerField.get()
                    val newHeader = oldHeader.copy(
                        modified = System.currentTimeMillis(),
                        schemas = this.schemas.values.map {
                            CatalogueHeader.SchemaRef(
                                it.name.simple,
                                null
                            )
                        }
                    )
                    this@DefaultCatalogue.headerField.compareAndSet(oldHeader, newHeader)
                    this@DefaultCatalogue.store.commit()
                } catch (e: DBException) {
                    this@Tx.status = TxStatus.ERROR
                    this@DefaultCatalogue.store.rollback()
                    throw DatabaseException("Failed to commit catalogue due to a storage exception: ${e.message}")
                }

                /* Materialize created schemas in enclosing Catalogue. */
                this.schemas.forEach {
                    if (!this@DefaultCatalogue.registry.contains(it.key)) {
                        this@DefaultCatalogue.registry[it.key] = it.value
                    }
                }

                /* Materialize dropped schemas in enclosing Catalogue. */
                val remove = this@DefaultCatalogue.registry.values.filter {
                    !this.schemas.containsKey(it.name)
                }
                remove.forEach {
                    try {
                        it.close()
                        FileUtilities.deleteRecursively(it.path)
                    } finally {
                        this@DefaultCatalogue.registry.remove(it.name)
                    }
                }
            }

            /* Delete newly created entities and commit store. */
            override fun rollback() {
                this.schemas.forEach {
                    if (!this@DefaultCatalogue.registry.contains(it.key)) {
                        it.value.close()
                        FileUtilities.deleteRecursively(it.value.path)
                    }
                }
            }
        }

        /**
         * Returns a list of [Name.SchemaName] held by this [DefaultCatalogue].
         *
         * @return [List] of all [Name.SchemaName].
         */
        override fun listSchemas(): List<Schema> = this.withReadLock {
            this.snapshot.schemas.values.toList()
        }

        /**
         * Returns the [Schema] for the given [Name.SchemaName].
         *
         * @param name [Name.SchemaName] to obtain the [Schema] for.
         */
        override fun schemaForName(name: Name.SchemaName): Schema = this.withReadLock {
            this.snapshot.schemas[name] ?: throw DatabaseException.SchemaDoesNotExistException(name)
        }

        /**
         * Creates a new, empty [Schema] with the given [Name.SchemaName] and [Path]
         *
         * @param name The [Name.SchemaName] of the new [Schema].
         */
        override fun createSchema(name: Name.SchemaName): Schema = this.withWriteLock {
            /* Check if schema with that name exists. */
            if (this.snapshot.schemas.contains(name))
                throw DatabaseException.SchemaAlreadyExistsException(name)

            try {
                /* Create empty folder for entity. */
                val data = this@DefaultCatalogue.path.resolve("schema_${name.simple}")
                if (!Files.exists(data)) {
                    Files.createDirectories(data)
                } else {
                    throw DatabaseException("Failed to create schema '$name'. Data directory '$data' seems to be occupied.")
                }

                /* Generate the store for the new schema. */
                val store =
                    this@DefaultCatalogue.config.mapdb.db(data.resolve(DefaultSchema.FILE_CATALOGUE))
                val schemaHeader =
                    store.atomicVar(DefaultSchema.SCHEMA_HEADER_FIELD, SchemaHeader.Serializer)
                        .create()
                schemaHeader.set(SchemaHeader(name.simple))
                store.commit()
                store.close()

                /* Add created schema to local snapshot. */
                val schema = DefaultSchema(data, this@DefaultCatalogue)
                this.snapshot.schemas[name] = schema
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
            val schema = this.snapshot.schemas[name]
                ?: throw DatabaseException.SchemaDoesNotExistException(name)

            if (this.context.lockOn(schema) == LockMode.NO_LOCK) {
                this.context.requestLock(schema, LockMode.EXCLUSIVE)
            }

            /* Remove dropped schema from local snapshot. */
            this.snapshot.schemas.remove(name)
            Unit
        }

        /**
         * Releases the [closeLock] on the [DefaultCatalogue].
         */
        override fun cleanup() {
            this@DefaultCatalogue.closeLock.unlockRead(this.closeStamp)
        }
    }
}