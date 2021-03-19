package org.vitrivr.cottontail.database.index

import org.junit.jupiter.api.Assertions
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.catalogue.CatalogueTest
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnEngine
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import java.nio.file.Files
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.stream.Collectors

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class AbstractIndexTest {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(AbstractIndexTest::class.java)
    }

    init {
        /* Assure existence of root directory. */
        if (!Files.exists(TestConstants.config.root)) {
            Files.createDirectories(TestConstants.config.root)
        }
    }

    /** [Name.SchemaName] of the test schema. */
    protected val schemaName = Name.SchemaName("test")

    /** [Name.EntityName] of the test schema. */
    protected val entityName = schemaName.entity("entity")

    /** The [ColumnDef]s of the columns in the test [Entity]. */
    protected abstract val columns: Array<ColumnDef<*>>

    /** The [ColumnDef] of the  test [Index]. */
    protected abstract val indexColumn: ColumnDef<*>

    /** [Name.IndexName] of the test [Index]. */
    protected abstract val indexName: Name.IndexName

    /** [IndexType] of the the test [Index]. */
    protected abstract val indexType: IndexType

    /** [IndexType] of the the test [Index]. */
    protected val indexParams: Map<String, String> = emptyMap()

    /** Catalogue used for testing. */
    private var catalogue: DefaultCatalogue = DefaultCatalogue(TestConstants.config)

    /** [Schema] used for testing. */
    protected var schema: Schema? = null

    /** [Entity] used for testing. */
    protected var entity: Entity? = null

    /** [Index] used for testing. */
    protected var index: Index? = null

    /** The [TransactionManager] used for this [CatalogueTest] instance. */
    protected val manager =
        TransactionManager(Executors.newFixedThreadPool(1) as ThreadPoolExecutor)

    /**
     * Initializes this [IndexTest] and prepares required [Entity] and [Index].
     */
    open fun initialize() {
        /* Prepare data structures. */
        this.schema = prepareSchema()
        this.entity = prepareEntity()
        this.index = prepareIndex()

        /* Populate database with data. */
        this.populateDatabase()

        /* Update the index. */
        this.updateIndex()
        log("Starting test...")
    }

    /**
     * Tears down this [IndexTest].
     */
    open fun teardown() {
        val pathsToDelete = Files.walk(TestConstants.config.root).sorted(Comparator.reverseOrder())
            .collect(Collectors.toList())
        pathsToDelete.forEach { Files.delete(it) }
    }

    /**
     * Prepares and returns an empty test [Schema].
     */
    protected fun prepareSchema(): Schema {
        log("Creating schema ${this.schemaName}.")
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val ret = catalogueTx.createSchema(this.schemaName)
        txn.commit()
        Assertions.assertTrue(Files.exists(ret.path))
        return ret
    }

    /**
     * Prepares and returns an empty test [Entity].
     */
    protected fun prepareEntity(): Entity {
        log("Creating schema ${this.entityName}.")
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        val schemaTx = txn.getTx(this.schema!!) as SchemaTx
        val ret = schemaTx.createEntity(this.entityName, *this.columns.map { it to ColumnEngine.MAPDB }.toTypedArray())
        txn.commit()
        Assertions.assertTrue(Files.exists(ret.path))
        return ret
    }

    /**
     * Prepares and returns an empty test [Index].
     */
    protected fun prepareIndex(): Index {
        log("Creating index ${this.indexName}.")
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        val entityTx = txn.getTx(this.entity!!) as EntityTx
        val ret = entityTx.createIndex(
            this.indexName,
            this.indexType,
            arrayOf(this.indexColumn),
            this.indexParams
        )
        txn.commit()
        Assertions.assertTrue(Files.exists(ret.path))
        return ret
    }

    /**
     * Updates all indexes.
     */
    protected fun updateIndex() {
        log("Updating index ${this.indexName}.")
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        val indexTx = txn.getTx(this.index!!) as IndexTx
        indexTx.rebuild()
        txn.commit()
    }


    /**
     * Populates the test database with data.
     */
    protected fun populateDatabase() {
        log("Inserting data (${TestConstants.collectionSize} items).")
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        val entityTx = txn.getTx(this.entity!!) as EntityTx

        /* Insert data and track how many entries have been stored for the test later. */
        for (i in 0..TestConstants.collectionSize) {
            entityTx.insert(nextRecord())
        }
        txn.commit()
    }

    /**
     * Logs an information message regarding this [AbstractIndexTest].
     */
    fun log(message: String) = LOGGER.info("Index test (${this.indexType}): $message")

    /**
     * Generates and returns a new [StandaloneRecord] for inserting into the database. Usually
     * generated randomly (which is up to the implementing class).
     */
    abstract fun nextRecord(): StandaloneRecord
}