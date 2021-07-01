package org.vitrivr.cottontail.database.index

import junit.framework.Assert.fail
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.config.Config
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
import org.vitrivr.cottontail.utilities.io.TxFileUtilities
import java.nio.file.Files

/**
 *
 * @author Ralph Gasser
 * @version 1.1.1
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractIndexTest {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(AbstractIndexTest::class.java)
    }

    /** [Config] used for this [AbstractIndexTest]. */
    private val config: Config = TestConstants.testConfig()

    init {
        /* Assure that root folder is empty! */
        if (Files.exists(this.config.root)) {
            TxFileUtilities.delete(this.config.root)
        }
        Files.createDirectories(this.config.root)
    }

    /** [Name.SchemaName] of the test schema. */
    protected val schemaName = Name.SchemaName("test")

    /** [Name.EntityName] of the test schema. */
    protected val entityName = schemaName.entity("index")

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
    protected var catalogue: DefaultCatalogue = DefaultCatalogue(this.config)

    /** The [TransactionManager] used for this [CatalogueTest] instance. */
    protected val manager = TransactionManager(
        this.config.execution.transactionTableSize,
        this.config.execution.transactionHistorySize
    )

    /**
     * Initializes this [AbstractIndexTest] and prepares required [Entity] and [Index].
     */
    @BeforeAll
    protected fun initialize() {
        /* Prepare data structures. */
        prepareSchema()
        prepareEntity()
        prepareIndex()

        /* Populate database with data. */
        this.populateDatabase()

        /* Update the index. */
        this.updateIndex()
        log("Starting test...")
    }

    /**
     * Tears down this [AbstractIndexTest].
     */
    @AfterAll
    protected fun teardown() {
        this.catalogue.close()
        TxFileUtilities.delete(this.config.root)
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
        return ret
    }

    /**
     * Prepares and returns an empty test [Entity].
     */
    protected fun prepareEntity() {
        log("Creating schema ${this.entityName}.")
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = txn.getTx(schema) as SchemaTx
        schemaTx.createEntity(this.entityName, *this.columns.map { it to ColumnEngine.MAPDB }.toTypedArray())
        txn.commit()
    }

    /**
     * Prepares and returns an empty test [Index].
     */
    protected fun prepareIndex() {
        log("Creating index ${this.indexName}.")
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = txn.getTx(schema) as SchemaTx
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = txn.getTx(entity) as EntityTx
        entityTx.createIndex(this.indexName, this.indexType, arrayOf(this.indexColumn), this.indexParams)
        txn.commit()
    }

    /**
     * Updates all indexes.
     */
    protected fun updateIndex() {
        log("Updating index ${this.indexName}.")
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = txn.getTx(schema) as SchemaTx
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = txn.getTx(entity) as EntityTx
        val index = entityTx.indexForName(this.indexName)
        val indexTx = txn.getTx(index) as IndexTx
        indexTx.rebuild()
        txn.commit()
    }


    /**
     * Populates the test database with data.
     */
    protected fun populateDatabase() {
        log("Inserting data (${TestConstants.collectionSize} items).")
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = txn.getTx(schema) as SchemaTx
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = txn.getTx(entity) as EntityTx

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
     * Tests for correct optimization of index.
     */
    @Test
    fun optimizationCountTest() {
        log("Optimizing entity ${this.entityName}.")

        /* Create entry. */
        val tx1 = this.manager.Transaction(TransactionType.SYSTEM)
        val catalogueTx = tx1.getTx(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = tx1.getTx(schema) as SchemaTx
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = tx1.getTx(entity) as EntityTx
        val preCount = entityTx.count()
        entityTx.optimize()
        tx1.commit()

        /* Test count. */
        val tx2 = this.manager.Transaction(TransactionType.SYSTEM)
        val countTx = tx2.getTx(entity) as EntityTx
        val postCount = countTx.count()
        if (postCount != preCount) {
            fail("optimizing caused elements to disappear")
        }
        countTx.commit()
    }

    /**
     * Generates and returns a new [StandaloneRecord] for inserting into the database. Usually
     * generated randomly (which is up to the implementing class).
     */
    abstract fun nextRecord(): StandaloneRecord
}