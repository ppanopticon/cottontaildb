package org.vitrivr.cottontail.legacy.v1.schema

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.mapdb.CottontailStoreWAL
import org.mapdb.DBException
import org.mapdb.Serializer
import org.mapdb.Store
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnEngine
import org.vitrivr.cottontail.database.column.mapdb.MapDBColumn
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.database.general.TxSnapshot
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.legacy.v1.catalogue.CatalogueV1
import org.vitrivr.cottontail.legacy.v1.entity.EntityV1
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.utilities.extensions.read
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.StampedLock

/**
 * Represents legacy (V1) [Schema] in Cottontail DB. Only around to support data migration.
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
class SchemaV1(override val name: Name.SchemaName, override val parent: CatalogueV1) : Schema {
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

    /** Reference to the [SchemaV1Header] of the [Schema]. */
    private val header
        get() = this.store.get(HEADER_RECORD_ID, SchemaV1Header.Serializer)
            ?: throw DatabaseException.DataCorruptionException("Failed to open header of schema $name!")

    /** A lock used to mediate access the closed state of this [Schema]. */
    private val closeLock = StampedLock()

    /** A map of loaded [EntityV1] references. */
    private val registry: MutableMap<Name.EntityName, EntityV1> =
        Collections.synchronizedMap(Object2ObjectOpenHashMap())

    /** The [DBOVersion] of this [SchemaV1]. */
    override val version: DBOVersion
        get() = DBOVersion.V1_0

    /** Flag indicating whether or not this [Schema] has been closed. */
    @Volatile
    override var closed: Boolean = false
        private set

    init {
        /* Initialize all entities. */
        this.header.entities.map {
            val name = this.name.entity(
                this.store.get(it, Serializer.STRING)
                    ?: throw DatabaseException.DataCorruptionException("Failed to read schema $name ($path): Could not find entity name of ID $it.")
            )
            this.registry[name] = EntityV1(name, this)
        }
    }

    /**
     * Creates and returns a new [SchemaTx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [SchemaTx] for.
     * @return New [SchemaTx]
     */
    override fun newTx(context: TransactionContext): SchemaTx = this.Tx(context)

    /**
     * Closes this [Schema] and all the [EntityV1] objects that are contained within.
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
        private val closeStamp = this@SchemaV1.closeLock.readLock()

        /** The [TxSnapshot] of this [SchemaTx]. */
        override val snapshot = object : TxSnapshot {
            override fun commit() =
                throw UnsupportedOperationException("Operation not supported on legacy DBO.")

            override fun rollback() =
                throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        /** Reference to the surrounding [Schema]. */
        override val dbo: DBO
            get() = this@SchemaV1

        /**
         * Returns a list of [EntityV1] held by this [Schema].
         *
         * @return [List] of all [Name.EntityName].
         */
        override fun listEntities(): List<Entity> = this.withReadLock {
            return this@SchemaV1.registry.values.toList()
        }

        /**
         * Returns an instance of [EntityV1] if such an instance exists. If the [EntityV1] has been loaded before,
         * that [EntityV1] is re-used. Otherwise, the [EntityV1] will be loaded from disk.
         *
         * @param name Name of the [EntityV1] to access.
         * @return [EntityV1] or null.
         */
        override fun entityForName(name: Name.EntityName): EntityV1 = this.withReadLock {
            return this@SchemaV1.registry[name]
                ?: throw DatabaseException.EntityDoesNotExistException(name)
        }

        override fun createEntity(
            name: Name.EntityName,
            vararg columns: Pair<ColumnDef<*>, ColumnEngine>
        ): DefaultEntity {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun dropEntity(name: Name.EntityName) {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        /**
         * Releases the [closeLock] on the [Schema].
         */
        override fun cleanup() {
            this@SchemaV1.closeLock.unlockRead(this.closeStamp)
        }
    }
}



