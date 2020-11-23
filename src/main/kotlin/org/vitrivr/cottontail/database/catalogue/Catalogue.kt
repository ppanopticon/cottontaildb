package org.vitrivr.cottontail.database.catalogue

import org.mapdb.DB
import org.mapdb.DBException
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.catalogue.entities.*
import org.vitrivr.cottontail.database.catalogue.serializers.ColumnNameSerializer
import org.vitrivr.cottontail.database.catalogue.serializers.EntityNameSerializer
import org.vitrivr.cottontail.database.catalogue.serializers.IndexNameSerializer
import org.vitrivr.cottontail.database.catalogue.serializers.SchemaNameSerializer
import org.vitrivr.cottontail.database.column.ColumnDriver
import org.vitrivr.cottontail.database.column.hare.HareColumn
import org.vitrivr.cottontail.database.column.mapdb.MapDBColumn
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import java.io.IOException
import java.lang.ref.SoftReference
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.stream.Collectors
import kotlin.concurrent.read
import kotlin.concurrent.write


/**
 * The main catalogue in Cottontail DB. It contains references to all the schemas, [Entity]s, [Column]s and [Index]es.
 *
 * @author Ralph Gasser
 * @version 1.2.0
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

        internal const val CATALOGUE_FIELD_NAME_HEADER = "header"

        /** Name for the [CatalogueSchema] entries.  */
        internal const val CATALOGUE_FIELD_NAME_SCHEMAS = "schemas"

        /** Filename for the [Entity] catalogue.  */
        internal const val CATALOGUE_FIELD_NAME_ENTITES = "entities"

        /** Filename for the [Entity] statistics catalogue.  */
        internal const val CATALOGUE_FIELD_NAME_ENTITES_STATISTICS = "entities_statistics"

        /** Filename for the [Entity] catalogue.  */
        internal const val CATALOGUE_FIELD_NAME_INDEXES = "indexes"

        /** Filename for the [Entity] catalogue.  */
        internal const val CATALOGUE_FIELD_NAME_COLUMNS = "columns"
    }

    /** Root to Cottontail DB root folder. */
    override val path: Path = this.config.root

    /** Constant name of the [Catalogue] object. */
    override val name: Name.RootName = Name.RootName

    /** The [Catalogue] this [Catalogue] belongs to, which is always itself. */
    override val catalogue: Catalogue = this

    /** Constant parent [DBO], which is null in case of the [Catalogue]. */
    override val parent: DBO? = null

    /** A lock used to mediate access to this [Catalogue]. */
    private val lock = ReentrantReadWriteLock()

    /** The [DB] that contains the Cottontail DB catalogue. */
    private val store = if (Files.exists(this.path.resolve(FILE_CATALOGUE))) {
        this.openStore()
    } else {
        this.initStore()
    }

    /** Timestamp of when this [Catalogue] was created. */
    private val header = this.store.atomicVar(CATALOGUE_FIELD_NAME_HEADER, CatalogueHeader.Serializer).open()

    /** List of schemas known by this [Catalogue]. */
    private val schemas = this.store.hashMap(CATALOGUE_FIELD_NAME_SCHEMAS, SchemaNameSerializer, CatalogueSchema.Serializer).counterEnable().open()

    /** List of entities known by this [Catalogue]. */
    private val entities = this.store.hashMap(CATALOGUE_FIELD_NAME_ENTITES, EntityNameSerializer, CatalogueEntity.Serializer).counterEnable().open()

    /** List of entities known by this [Catalogue]. */
    private val statistics = this.store.hashMap(CATALOGUE_FIELD_NAME_ENTITES_STATISTICS, EntityNameSerializer, CatalogueEntityStatistics.Serializer).counterEnable().open()

    /** List of columns known by this [Catalogue]. */
    private val columns = this.store.hashMap(CATALOGUE_FIELD_NAME_COLUMNS, ColumnNameSerializer, CatalogueColumn.Serializer).counterEnable().open()

    /** List of indexes known by this [Catalogue]. */
    private val indexes = this.store.hashMap(CATALOGUE_FIELD_NAME_INDEXES, IndexNameSerializer, CatalogueIndex.Serializer).counterEnable().open()

    /** A map of opened [Entity] references. The [SoftReference] allow for coping with changing memory conditions. */
    private val open = ConcurrentHashMap<Name.EntityName, SoftReference<Entity>>()

    /** Status indicating whether this [Catalogue] is open or closed. */
    @Volatile
    override var closed: Boolean = false
        private set

    /** Initialization logic for [Catalogue]. */
    init {
        /* Sanity check; has store been closed properly? */
        val header = this.header.get()
        if (header.lastOpened > header.lastClosed) {
            /* TODO: Sanity check due to improper close. */
        }

        /* Existence of schemas. */
        for (schema in this.schemas) {
            if (!Files.exists(this.path)) {
                throw DatabaseException.DataCorruptionException("Broken catalogue entry for schema '${schema.key}'. Path $path does not exist!")
            }
        }

        /* Existence of schemas. */
        for (schema in this.entities) {
            if (!Files.exists(this.path)) {
                throw DatabaseException.DataCorruptionException("Broken catalogue entry for schema '${schema.key}'. Path $path does not exist!")
            }
        }

        /* Update the opened timestamp & commit. */
        this.header.set(header.copy(lastOpened = System.currentTimeMillis()))
        this.store.commit()
    }

    /**
     * Closes the [Catalogue] and all objects contained within.
     */
    override fun close() = this.lock.write {
        /* Close all open entities. */
        this.open.forEach { (_, v) -> v.get()?.close() }

        /* Update last close timestamp and commit. */
        this.header.set(this.header.get().copy(lastClosed = System.currentTimeMillis()))
        this.store.commit()
        this.store.close()

        /* Set closed flag to true. */
        this.closed = true
    }

    /**
     * Handles finalization of the [Catalogue].
     */
    protected fun finalize() {
        if (!this.closed) {
            this.close() /* This should not happen! */
        }
    }

    /**
     * Creates a new, empty schema name with the given [Name.SchemaName]
     *
     * @param name The [Name.SchemaName] of the new [Schema].
     * @param location The [Path] to the location of the new [Schema]
     */
    fun createSchema(name: Name.SchemaName, location: Path = this.path) = this.lock.write {
        check(!this.closed) { "Catalogue has been closed and cannot be used anymore." }

        /* Check if schema with that name exists. */
        if (this.schemas.containsKey(name)) {
            throw DatabaseException.SchemaAlreadyExistsException(name)
        }

        /* Create empty folder for entity. */
        val path = location.resolve("schema_${name.simple}")
        try {
            if (!Files.exists(path)) {
                Files.createDirectories(path)
            } else {
                throw DatabaseException("Failed to create schema '$name'. Data directory '$path' seems to be occupied.")
            }
        } catch (e: IOException) {
            throw DatabaseException("Failed to create schema '$name' due to an IO exception: ${e.message}")
        }

        /* Generate catalogue entries. */
        try {
            this.header.set(this.header.get().copy(modified = System.currentTimeMillis()))
            this.schemas[name] = CatalogueSchema(path, emptyList())
            this.store.commit()
        } catch (e: DBException) {
            this.store.rollback()
            val pathsToDelete = Files.walk(path).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
            pathsToDelete.forEach { Files.delete(it) }
            throw DatabaseException("Failed to create schema '$name' due to a storage exception: ${e.message}")
        }
    }

    /**
     * Drops an existing [Schema] with the given [Name.SchemaName].
     *
     * <strong>Warning:</strong> Dropping a [Schema] deletes all the files associated with it [Schema]!
     *
     * @param schemaName The [Name.SchemaName] of the [Schema] to be dropped.
     */
    fun dropSchema(schemaName: Name.SchemaName) = this.lock.write {
        check(!this.closed) { "Catalogue has been closed and cannot be used anymore." }

        /* Try to close all open entities for the schema. */
        try {
            val schema: CatalogueSchema = this.schemas[schemaName]
                    ?: throw DatabaseException.SchemaDoesNotExistException(schemaName)
            for (entityName in schema.entities) {
                if (this.open.containsKey(entityName)) {
                    this.open[entityName]?.get()?.close()
                    this.open.remove(entityName)
                }
            }

            /* Drop all relevant catalogue entries. */
            this.header.set(this.header.get().copy(modified = System.currentTimeMillis()))
            for (entityName in schema.entities) {
                val entityCatalogue = this.entities[entityName]
                        ?: throw DatabaseException.EntityDoesNotExistException(entityName)
                for (indexName in entityCatalogue.indexes) {
                    this.indexes.remove(indexName)
                }
                for (columnName in entityCatalogue.columns) {
                    this.columns.remove(columnName)
                }
                this.entities.remove(entityName)
            }
            this.schemas.remove(schemaName) /* Remove schema entry. */

            /* Update header. */
            this.header.set(this.header.get().copy(modified = System.currentTimeMillis()))
            this.store.commit()

            /* Delete files that belong to the schema. */
            val path = this.path.resolve("schema_${schemaName.simple}")
            val pathsToDelete = Files.walk(path).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
            pathsToDelete.forEach { Files.delete(it) }
        } catch (e: DBException) {
            this.store.rollback()
            throw DatabaseException("Failed to drop schema '$schemaName' due to a storage exception: ${e.message}")
        } catch (e: IOException) {
            throw DatabaseException("Failed to drop schema '$schemaName' due to an IO exception: ${e.message}")
        }
    }

    /**
     * Creates a new [Entity] in this [Schema].
     *
     * @param entityName The name of the [Entity] that should be created.
     * @param columns The list of columns that should be created.
     */
    fun createEntity(entityName: Name.EntityName, vararg columns: ColumnDef<*>) {
        val colWithDriver = columns.map { Pair(it, ColumnDriver.HARE) }.toTypedArray()
        createEntity(entityName, *colWithDriver)
    }

    /**
     * Creates a new [Entity] in this [Schema].
     *
     * @param entityName The name of the [Entity] that should be created.
     * @param columns The list of columns that should be created.
     */
    fun createEntity(entityName: Name.EntityName, vararg columns: Pair<ColumnDef<*>, ColumnDriver>) = this.lock.write {
        check(!this.closed) { "Catalogue has been closed and cannot be used anymore." }

        /* Obtain schema entry. */
        if (this.entities.containsKey(entityName)) throw DatabaseException.EntityAlreadyExistsException(entityName)
        val schema = this.schemas[entityName.schema()]
                ?: throw DatabaseException.SchemaDoesNotExistException(entityName.schema())
        val location = schema.path.resolve("entity_${entityName.simple}")

        try {
            /* Prepare file location. */
            if (!Files.exists(location)) {
                Files.createDirectories(location)
            } else {
                throw DatabaseException("Failed to create entity '$name'. Data directory '$location' seems to be occupied.")
            }

            /* Prepare columns + column entries in catalogue. */
            val columnNames = columns.map {
                if (this.columns.containsKey(it.first.name)) {
                    throw DatabaseException.DuplicateColumnException(it.first.name)
                }
                val path = when (it.second) {
                    ColumnDriver.MAPDB -> MapDBColumn.initialize(it.first, location, this.config.mapdb)
                    ColumnDriver.HARE -> HareColumn.initialize(it.first, location, this.config.hare)
                }
                this.columns[it.first.name] = CatalogueColumn(path, it.first.type, it.first.logicalSize, it.first.nullable, it.second)
                it.first.name
            }

            /* Prepare entity entry in catalogue */
            this.entities[entityName] = CatalogueEntity(location, columnNames, emptyList())

            /* Update header. */
            this.header.set(this.header.get().copy(modified = System.currentTimeMillis()))
            this.store.commit()
        } catch (e: DBException) {
            this.store.rollback()
            val pathsToDelete = Files.walk(location).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
            pathsToDelete.forEach { Files.delete(it) }
            throw DatabaseException("Failed to create entity '$name' due to error in the underlying data store: {${e.message}")
        } catch (e: IOException) {
            throw DatabaseException("Failed to create entity '$name' due to an IO exception: {${e.message}")
        }
    }

    /**
     * Drops an [Entity] in this [Schema]. The act of dropping an [Entity] requires a lock on that [Entity].
     *
     * @param entityName The name of the [Entity] that should be dropped.
     */
    fun dropEntity(entityName: Name.EntityName) = this.lock.write {
        check(!this.closed) { "Catalogue has been closed and cannot be used anymore." }

        /* Try to close all open entities for the schema. */
        val entityCatalogue: CatalogueEntity = this.entities[entityName]
                ?: throw DatabaseException.EntityDoesNotExistException(entityName)
        if (this.open.containsKey(entityName)) {
            this.open[entityName]?.get()?.close()
            this.open.remove(entityName)
        }

        try {
            /* Update relevant catalogue entries. */
            for (indexName in entityCatalogue.indexes) {
                this.indexes.remove(indexName)
            }
            for (columnName in entityCatalogue.columns) {
                this.columns.remove(columnName)
            }
            this.entities.remove(entityName)

            /* Update header. */
            this.header.set(this.header.get().copy(modified = System.currentTimeMillis()))
            this.store.commit()

            /* Delete all files associated with the entity. */
            val pathsToDelete = Files.walk(this.path.resolve("entity_${name.simple}")).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
            pathsToDelete.forEach { Files.deleteIfExists(it) }
        } catch (e: DBException) {
            this.store.rollback()
            throw DatabaseException("Failed to drop entity '$entityName' due to a storage exception: ${e.message}")
        }
    }

    /**
     * Creates the [Index] with the given settings
     *
     * @param indexName [Name.IndexName] of the [Index] to create.
     * @param type Type of the [Index] to create.
     * @param columns The list of [columns] to [Index].
     */
    fun createIndex(indexName: Name.IndexName, type: IndexType, columns: Array<ColumnDef<*>>, params: Map<String, String> = emptyMap()) = this.lock.write {
        check(!this.closed) { "Catalogue has been closed and cannot be used anymore." }

        /* Check if index already exists. */
        if (this.indexes.containsKey(indexName)) {
            throw DatabaseException.IndexAlreadyExistsException(indexName)
        }

        val path = this.entities[indexName.entity()]?.path?.parent?.resolve("idx_${indexName.simple}.db")
                ?: throw DatabaseException.EntityDoesNotExistException(indexName.entity())
        try {
            /* Create index entry in catalogue. */
            this.indexes[indexName] = CatalogueIndex(path, type, columns.map { it.name })

            /* Prepare index. */
            type.create(indexName, this.catalogue, columns, params)

            /* Update header. */
            this.header.set(this.header.get().copy(modified = System.currentTimeMillis()))
            this.store.commit()
        } catch (e: Exception) {
            this.store.rollback()
            val pathsToDelete = Files.walk(path).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
            pathsToDelete.forEach { Files.delete(it) }
            throw DatabaseException("Failed to create index '$name' due to a storage exception: ${e.message}")
        }
    }


    /**
     * Drops the [Index] with the given name.
     *
     * @param indexName [Name.IndexName] of the [Index] to drop.
     */
    fun dropIndex(indexName: Name.IndexName) = this.lock.write {
        check(!this.closed) { "Catalogue has been closed and cannot be used anymore." }

        /* Try to close all open entities for the schema. */
        val indexCatalogue: CatalogueIndex = this.indexes[indexName]
                ?: throw DatabaseException.IndexDoesNotExistException(indexName)
        if (this.open.containsKey(indexName.entity())) {
            /* TODO: Close the index. */
        }

        try {
            /* Create index entry in catalogue. */
            this.indexes.remove(indexName)

            /* Update header. */
            this.header.set(this.header.get().copy(modified = System.currentTimeMillis()))
            this.store.commit()

            /* Delete index files.*/
            val pathsToDelete = Files.walk(indexCatalogue.path).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
            pathsToDelete.forEach { Files.delete(it) }
        } catch (e: DBException) {
            this.store.rollback()
        }
    }

    /**
     * Returns a list of all [Name.EntityName] entries.
     *
     * @return [List] of [Name.EntityName]
     */
    fun allEntities(): List<Name.EntityName> = this.lock.read {
        this.entities.keys.map { it }
    }

    /**
     * Returns a list of all [Name.SchemaName] entries.
     *
     * @return [List] of [Name.SchemaName]
     */
    fun allSchemas(): List<Name.SchemaName> = this.lock.read {
        this.schemas.keys.map { it }
    }

    /**
     * Returns a list of all [Name.ColumnName] entries.
     *
     * @return [List] of [Name.ColumnName]
     */
    fun allColumns(): List<Name.ColumnName> = this.lock.read {
        this.columns.keys.map { it }
    }

    /**
     * Returns a list of all [Name.IndexName] entries.
     *
     * @return [List] of [Name.IndexName]
     */
    fun allIndexes(): List<Name.IndexName> = this.lock.read {
        this.indexes.keys.map { it }
    }

    /**
     * Fetches and returns the [CatalogueSchema] entry for the provided [Name.SchemaName].
     *
     * @param entityName [Name.SchemaName] to lookup the [CatalogueSchema] for.
     * @return [CatalogueSchema]
     */
    fun schemaForName(schemaName: Name.SchemaName): CatalogueSchema = this.lock.read {
        check(!this.closed) { "Catalogue has been closed and cannot be used anymore." }
        return this.schemas[schemaName]
                ?: throw DatabaseException.SchemaDoesNotExistException(schemaName)
    }

    /**
     * Fetches and returns the [CatalogueEntity] entry for the provided [Name.EntityName].
     *
     * @param entityName [Name.EntityName] to lookup the [CatalogueEntity] for.
     * @return [CatalogueEntity]
     */
    fun entityForName(entityName: Name.EntityName): CatalogueEntity = this.lock.read {
        check(!this.closed) { "Catalogue has been closed and cannot be used anymore." }
        return this.entities[entityName]
                ?: throw DatabaseException.EntityDoesNotExistException(entityName)
    }

    /**
     * Fetches and returns the [CatalogueColumn] entry for the provided [Name.ColumnName].
     *
     * @param columnName [Name.ColumnName] to lookup the [CatalogueColumn] for.
     * @return [CatalogueColumn]
     */
    fun columnForName(columnName: Name.ColumnName): CatalogueColumn = this.lock.read {
        check(!this.closed) { "Catalogue has been closed and cannot be used anymore." }
        return this.columns[columnName]
                ?: throw DatabaseException.ColumnDoesNotExistException(columnName)
    }

    /**
     * Fetches and returns the [CatalogueIndex] entry for the provided [Name.IndexName].
     *
     * @param indexName [Name.IndexName] to lookup the [CatalogueIndex] for.
     * @return [CatalogueIndex]
     */
    fun indexForName(indexName: Name.IndexName): CatalogueIndex = this.lock.read {
        check(!this.closed) { "Catalogue has been closed and cannot be used anymore." }
        return this.indexes[indexName]
                ?: throw DatabaseException.IndexDoesNotExistException(indexName)
    }

    /**
     * Fetches and returns the [CatalogueEntityStatistics] entry for the provided [Name.EntityName].
     *
     * @param entityName [Name.EntityName] to lookup the [CatalogueEntityStatistics] for.
     * @return [CatalogueIndex]
     */
    fun statisticsForName(entityName: Name.EntityName): CatalogueEntityStatistics = this.lock.read {
        check(!this.closed) { "Catalogue has been closed and cannot be used anymore." }
        return this.statistics[entityName]
                ?: throw DatabaseException.EntityDoesNotExistException(entityName)
    }

    /**
     * Updates the [CatalogueEntityStatistics] entry for the provided [Name.EntityName].
     *
     * @param entityName [Name.EntityName] to update the [CatalogueEntityStatistics] for.
     * @param statistics [CatalogueEntityStatistics] to update.
     *
     * @return [CatalogueIndex]
     */
    internal fun updateStatistics(entityName: Name.EntityName, statistics: CatalogueEntityStatistics) {
        check(!this.closed) { "Catalogue has been closed and cannot be used anymore." }
        this.statistics[entityName] = statistics
    }

    /**
     * Returns an instance of [Entity] if such an instance exists. If the [Entity] has been
     * loaded before, that [Entity] is re-used. Otherwise, the [Entity] will be loaded from disk.
     *
     * @param entityName Name of the [Entity] to access.
     */
    fun instantiateEntity(entityName: Name.EntityName): Entity = this.lock.read {
        /* Check closed status. */
        check(!this.closed) { "Catalogue has been closed and cannot be used anymore." }

        if (!this.entities.containsKey(entityName)) throw DatabaseException.EntityDoesNotExistException(entityName)
        var ret = this.open[entityName]?.get()
        if (ret == null) {
            ret = Entity(entityName, this)
            this.open[entityName] = SoftReference(ret)
        }
        ret
    }

    /**
     * Opens the [DB] underlying this Cottontail DB [Catalogue]
     *
     * @return [DB] object.
     */
    private fun openStore(): DB = try {
        this.config.mapdb.db(this.path.resolve(FILE_CATALOGUE))
    } catch (e: DBException) {
        throw DatabaseException("Failed to open Cottontail DB catalogue: ${e.message}'.")
    }

    /**
     * Initializes a new Cottontail DB [Catalogue] [DB] object.
     *
     * @return [DB] object.
     */
    private fun initStore() = try {
        try {
            if (!Files.exists(this.path)) {
                Files.createDirectories(this.path)
            }
        } catch (e: IOException) {
            throw DatabaseException("Failed to create Cottontail DB catalogue due to an IO exception: ${e.message}")
        }

        /* Create and initialize new store. */
        val store = this.config.mapdb.db(this.path.resolve(FILE_CATALOGUE))
        store.atomicVar(CATALOGUE_FIELD_NAME_HEADER, CatalogueHeader.Serializer, CatalogueHeader()).create()
        store.hashMap(CATALOGUE_FIELD_NAME_SCHEMAS, SchemaNameSerializer, CatalogueSchema.Serializer).counterEnable().create()
        store.hashMap(CATALOGUE_FIELD_NAME_ENTITES, EntityNameSerializer, CatalogueEntity.Serializer).counterEnable().create()
        store.hashMap(CATALOGUE_FIELD_NAME_ENTITES_STATISTICS, EntityNameSerializer, CatalogueEntityStatistics.Serializer).counterEnable().create()
        store.hashMap(CATALOGUE_FIELD_NAME_COLUMNS, ColumnNameSerializer, CatalogueColumn.Serializer).counterEnable().create()
        store.hashMap(CATALOGUE_FIELD_NAME_INDEXES, IndexNameSerializer, CatalogueIndex.Serializer).counterEnable().create()

        store.commit()
        store
    } catch (e: DBException) {
        throw DatabaseException("Failed to initialize the Cottontail DB catalogue: ${e.message}'.")
    }
}