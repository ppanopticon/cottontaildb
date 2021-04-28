package org.vitrivr.cottontail.database.catalogue

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.schema.DefaultSchema
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.utilities.io.TxFileUtilities
import java.nio.file.Files
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor

/**
 * A set of unit tests to test basic [DefaultCatalogue] functionality.
 *
 * @author Ralph Gasser
 * @version 1.1.1
 */
class CatalogueTest {

    /** [Name.SchemaName] used for this [CatalogueTest]. */
    private val schemaName = Name.SchemaName("catalogue-test")

    /** [Config] used for this [CatalogueTest]. */
    private val config: Config = TestConstants.testConfig()

    init {
        /* Assure that root folder is empty! */
        if (Files.exists(this.config.root)) {
            TxFileUtilities.delete(this.config.root)
        }
        Files.createDirectories(this.config.root)
    }

    /** The [DefaultCatalogue] object to run the test with. */
    private val catalogue: DefaultCatalogue = DefaultCatalogue(this.config)

    /** The [TransactionManager] used for this [CatalogueTest] instance. */
    private val manager = TransactionManager(
        Executors.newFixedThreadPool(1) as ThreadPoolExecutor,
        this.config.execution.transactionTableSize,
        this.config.execution.transactionHistorySize
    )

    @AfterEach
    fun teardown() {
        this.catalogue.close()
        TxFileUtilities.delete(this.config.root)
    }

    /**
     * Creates a new [DefaultSchema] and runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun createSchemaCommitTest() {
        /* Transaction 1: Create schema. */
        val txn1 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTxn1 = txn1.getTx(this.catalogue) as CatalogueTx
            catalogueTxn1.createSchema(this.schemaName)
            txn1.commit()
        } catch (t: Throwable) {
            txn1.rollback()
            throw t
        }

        /* Transaction 2: Read and compare schema. */
        val txn2 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTxn2 = txn2.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTxn2.schemaForName(this.schemaName)
            val schemaTxn2 = txn2.getTx(schema) as SchemaTx

            /* Check if schema directory exists. */
            Assertions.assertTrue(Files.isReadable(schema.path))
            Assertions.assertTrue(Files.isDirectory(schema.path))

            /* Check if schema contains the expected number of entities (zero). */
            Assertions.assertEquals(0, schemaTxn2.listEntities().size)
        } finally {
            txn2.rollback()
        }
    }

    /**
     * Creates a new [DefaultSchema] and runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun createSchemaRollbackTest() {
        /* Transaction 1: Create schema. */
        val txn1 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTxn1 = txn1.getTx(this.catalogue) as CatalogueTx
            catalogueTxn1.createSchema(this.schemaName)
        } finally {
            txn1.rollback()
        }

        /* Transaction 2: Read and compare schema. */
        val txn2 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTxn2 = txn2.getTx(this.catalogue) as CatalogueTx

            /* Read schema (should throw error). */
            Assertions.assertThrows(DatabaseException.SchemaDoesNotExistException::class.java) {
                catalogueTxn2.schemaForName(this.schemaName)
            }
        } finally {
            txn2.rollback()
        }
    }

    /**
     * Creates a new [DefaultSchema] and then drops it.
     *
     * Runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun dropSchemaCommitTest() {
        /* Transaction 1: Create schema. */
        val txn1 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTxn1 = txn1.getTx(this.catalogue) as CatalogueTx
            catalogueTxn1.createSchema(this.schemaName)
            txn1.commit()
        } catch (t: Throwable) {
            txn1.rollback()
            throw t
        }


        /* Transaction 2: Read, check and drop schema. */
        val txn2 = this.manager.Transaction(TransactionType.SYSTEM)
        val path = try {
            val catalogueTxn2 = txn2.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTxn2.schemaForName(this.schemaName)

            /* Check if schema directory exists. */
            Assertions.assertTrue(Files.isReadable(schema.path))
            Assertions.assertTrue(Files.isDirectory(schema.path))

            /* Drop schema. */
            catalogueTxn2.dropSchema(this.schemaName)
            txn2.commit()
            schema.path
        } catch (t: Throwable) {
            txn2.rollback()
            throw t
        }


        /* Transaction 3: Read and compare schema. */
        val txn3 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTxn3 = txn3.getTx(this.catalogue) as CatalogueTx

            /* Read schema (should throw error). */
            Assertions.assertThrows(DatabaseException.SchemaDoesNotExistException::class.java) {
                catalogueTxn3.schemaForName(this.schemaName)
            }

            /* File / folder for schema must not exist! */
            Assertions.assertFalse(Files.exists(path))
        } finally {
            txn3.rollback()
        }
    }

    /**
     * Creates a new [DefaultSchema] and then drops it followed by a rollback.
     *
     * Runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun dropSchemaRollbackTest() {
        /* Transaction 1: Create schema. */
        val txn1 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTxn1 = txn1.getTx(this.catalogue) as CatalogueTx
            catalogueTxn1.createSchema(this.schemaName)
            txn1.commit()
        } catch (t: Throwable) {
            txn1.rollback()
            throw t
        }


        /* Transaction 2: Read, check and drop schema. */
        val txn2 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTxn2 = txn2.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTxn2.schemaForName(this.schemaName)

            /* Check if schema directory exists. */
            Assertions.assertTrue(Files.isReadable(schema.path))
            Assertions.assertTrue(Files.isDirectory(schema.path))

            /* Drop schema. */
            catalogueTxn2.dropSchema(this.schemaName)
        } finally {
            txn2.rollback()
        }


        /* Transaction 3: Read and compare schema. */
        val txn3 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTxn3 = txn3.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTxn3.schemaForName(this.schemaName)
            Assertions.assertNotNull(schema)
            Assertions.assertTrue(Files.isDirectory(schema.path))
        } finally {
            txn3.rollback()
        }
    }

    /**
     * Creates a new [DefaultSchema] and then drops it.
     *
     * Runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun createAndDropSchemaSingleTransactionTest() {
        /* Transaction 1: Create schema. */
        val txn1 = this.manager.Transaction(TransactionType.SYSTEM)
        val path = try {
            val catalogueTxn1 = txn1.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTxn1.createSchema(this.schemaName)
            catalogueTxn1.dropSchema(this.schemaName)
            schema.path
        } finally {
            txn1.commit()
        }

        /* Transaction 2: Read and compare schema. */
        val txn3 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTxn3 = txn3.getTx(this.catalogue) as CatalogueTx

            /* Read schema (should throw error). */
            Assertions.assertThrows(DatabaseException.SchemaDoesNotExistException::class.java) {
                catalogueTxn3.schemaForName(this.schemaName)
            }

            /* File / folder for schema must not exist! */
            Assertions.assertFalse(Files.isDirectory(path))
            Assertions.assertFalse(Files.exists(path))
        } finally {
            txn3.rollback()
        }
    }
}
