package org.vitrivr.cottontail.database.catalogue

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.mapdb.DB
import org.mapdb.StoreWAL
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.general.*
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.database.schema.DefaultSchema
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.TxException
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import org.vitrivr.cottontail.utilities.io.TxFileUtilities
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.*
import java.util.concurrent.locks.StampedLock

/**
 * The default [Catalogue] implementation based on Map DB.
 *
 * @author Ralph Gasser
 * @version 2.1.0
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
    private val registry: MutableMap<Name.SchemaName, Schema> = Collections.synchronizedMap(Object2ObjectOpenHashMap())

    /** Size of this [DefaultCatalogue] in terms of [Schema]s it contains. This is a snapshot and may change anytime! */
    override val size: Int
        get() = this.closeLock.read { this.headerField.get().schemas.size }

    /** Status indicating whether this [DefaultCatalogue] is open or closed. */
    override val closed: Boolean
        get() = this.store.isClosed()

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
                    throw DatabaseException.DataCorruptionException("Broken catalogue entry for schema '${schemaRef.name}'. Path $path does not exist!")
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
    override fun close() = this.closeLock.write {
        this.store.close()
        this.registry.forEach { (_, v) -> v.close() }
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

        /**
         * The [CatalogueTxSnapshot] of this [CatalogueTx].
         *
         * Important: The [CatalogueTxSnapshot] is created lazily upon first access, which means that whatever
         * caller creates it, it holds the necessary locks!
         */
        override val snapshot by lazy {
            object : CatalogueTxSnapshot {
                override val schemas = Object2ObjectOpenHashMap(this@DefaultCatalogue.registry)

                /** A map of all [TxAction] executed by this [CatalogueTxSnapshot]. Can be seen as an in-memory WAL. */
                override val actions = LinkedList<TxAction>()

                /* Make changes to indexes available to entity and persist them. */
                override fun commit() {
                    try {
                        /* Materialize changes in surrounding schema (in-memory). */
                        this.actions.forEach { it.commit() }

                        /* Update update header and commit changes. */
                        val oldHeader = this@DefaultCatalogue.headerField.get()
                        val newHeader = oldHeader.copy(
                                modified = System.currentTimeMillis(),
                                schemas = this.schemas.values.map { CatalogueHeader.SchemaRef(it.name.simple, null) }
                        )
                        this@DefaultCatalogue.headerField.compareAndSet(oldHeader, newHeader)
                        this@DefaultCatalogue.store.commit()
                    } catch (e: Throwable) {
                        this@Tx.status = TxStatus.ERROR
                        this@DefaultCatalogue.store.rollback()
                        throw DatabaseException("Failed to commit catalogue due to a storage exception: ${e.message}")
                    }
                }

                /**
                 * Rolls back this [CatalogueTx] and reverts all changes made through it.
                 */
                override fun rollback() {
                    this.actions.forEach { it.rollback() }
                    this@DefaultCatalogue.store.rollback()
                }

                /**
                 * Records a [TxAction] with this [TxSnapshot].
                 *
                 * @param action The [TxAction] to record.
                 * @return True on success, false otherwise.
                 */
                override fun record(action: TxAction): Boolean = when (action) {
                    is CreateSchemaTxAction,
                    is DropSchemaTxAction -> {
                        this.actions.add(action)
                        true
                    }
                    else -> false
                }
            }
        }

        /** Checks if DBO is still open. */
        init {
            if (this@DefaultCatalogue.closed) {
                this@DefaultCatalogue.closeLock.unlockRead(this.closeStamp)
                throw TxException.TxDBOClosedException(this.context.txId, this@DefaultCatalogue)
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
            if (this.snapshot.schemas.contains(name)) throw DatabaseException.SchemaAlreadyExistsException(name)

            /* Initialize schema on disk */
            try {
                val data = DefaultSchema.initialize(name, this@DefaultCatalogue.path, this@DefaultCatalogue.config)
                val schema = DefaultSchema(data, this@DefaultCatalogue)
                this.snapshot.record(CreateSchemaTxAction(schema))
                this.snapshot.schemas[name] = schema
                return schema
            } catch (e: DatabaseException) {
                this.status = TxStatus.ERROR
                throw e
            }
        }

        /**
         * Drops an existing [Schema] with the given [Name.SchemaName].
         *
         * @param name The [Name.SchemaName] of the [Schema] to be dropped.
         */
        override fun dropSchema(name: Name.SchemaName) = this.withWriteLock {
            /* Obtain schema and acquire exclusive lock on it. */
            val schema = this.snapshot.schemas[name] ?: throw DatabaseException.SchemaDoesNotExistException(name)
            this.context.requestLock(schema, LockMode.EXCLUSIVE)


            /* Remove dropped schema from local snapshot. */
            this.snapshot.record(DropSchemaTxAction(name))
            this.snapshot.schemas.remove(name)
            Unit
        }

        /**
         * Releases the [closeLock] on the [DefaultCatalogue].
         */
        override fun cleanup() {
            this@DefaultCatalogue.closeLock.unlockRead(this.closeStamp)
        }

        /**
         * A [TxAction] for creating a new [Schema].
         *
         * @param schema [Schema] that has been created.
         */
        inner class CreateSchemaTxAction(private val schema: Schema) : TxAction {
            override fun commit() {
                this.schema.close()
                val move = Files.move(this.schema.path, TxFileUtilities.plainPath(this.schema.path), StandardCopyOption.ATOMIC_MOVE)
                this@DefaultCatalogue.registry[this.schema.name] = DefaultSchema(move, this.schema.parent)
            }

            override fun rollback() {
                this.schema.close()
                TxFileUtilities.delete(this.schema.path)
            }
        }

        /**
         * A [TxAction] implementation for dropping an [Schema].
         *
         * @param schema [Schema] that has been dropped.
         */
        inner class DropSchemaTxAction(private val schema: Name.SchemaName) : TxAction {
            override fun commit() {
                val schema = this@DefaultCatalogue.registry.remove(this.schema) ?: throw IllegalStateException("Failed to drop schema $schema because it is unknown to catalogue. This is a programmer's error!")
                schema.close()
                if (Files.exists(schema.path)) {
                    TxFileUtilities.delete(schema.path)
                }
            }

            override fun rollback() { /* No op. */
            }
        }
    }
}