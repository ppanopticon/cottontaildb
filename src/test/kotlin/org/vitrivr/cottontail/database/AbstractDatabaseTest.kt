package org.vitrivr.cottontail.database

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.catalogue.CatalogueTest
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnEngine
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.utilities.io.TxFileUtilities
import java.nio.file.Files

/**
 * Abstract class for unit tests that require a Cottontail DB database.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractDatabaseTest {
    companion object {
        private val LOGGER = LoggerFactory.getLogger(AbstractDatabaseTest::class.java)
    }
    
    /** [Config] used for this [AbstractDatabaseTest]. */
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
    abstract protected val entities: List<Pair<Name.EntityName, List<ColumnDef<*>>>>
    /** Catalogue used for testing. */
    protected var catalogue: DefaultCatalogue = DefaultCatalogue(this.config)

    /** The [TransactionManager] used for this [CatalogueTest] instance. */
    protected val manager = TransactionManager(
        this.config.execution.transactionTableSize,
        this.config.execution.transactionHistorySize
    )

    /**
     * Initializes this [AbstractDatabaseTest] and prepares required [Entity] and [Index].
     */
    @BeforeAll
    protected fun initialize() {
        /* Update the index. */
        LOGGER.info("Preparing database...")

        /* Prepare data structures. */
        prepareSchema()
        prepareEntity()

        /* Populate database with data. */
        this.populateDatabase()

        /* Update the index. */
        LOGGER.info("Starting test...")
    }


    /**
     * Tears down this [AbstractDatabaseTest].
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
        LOGGER.info("Creating schema ${this.schemaName}.")
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
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        for (e in this.entities) {
            LOGGER.info("Creating schema ${e.first}.")
            val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = txn.getTx(schema) as SchemaTx
            schemaTx.createEntity(e.first, *e.second.map { it to ColumnEngine.MAPDB }.toTypedArray())
        }
        txn.commit()
    }

    /**
     * Populates database with test data.
     */
    protected abstract fun populateDatabase()
}