package org.vitrivr.cottontail.database.catalogue

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.schema.DefaultSchema
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import java.nio.file.Files
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.stream.Collectors

/**
 * A set of unit tests to test basic [DefaultCatalogue] functionality.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class CatalogueTest {
    private val schemaName = Name.SchemaName("schema-test")

    init {
        if (!Files.exists(TestConstants.config.root)) {
            Files.createDirectories(TestConstants.config.root)
        }
    }

    /** The [DefaultCatalogue] object to run the test with. */
    private val catalogue: DefaultCatalogue = DefaultCatalogue(TestConstants.config)

    /** The [TransactionManager] used for this [CatalogueTest] instance. */
    private val manager = TransactionManager(Executors.newFixedThreadPool(1) as ThreadPoolExecutor)

    @AfterEach
    fun teardown() {
        this.catalogue.close()
        val pathsToDelete = Files.walk(TestConstants.config.root).sorted(Comparator.reverseOrder())
            .collect(Collectors.toList())
        pathsToDelete.forEach { Files.delete(it) }
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
    fun dropSchemaTest() {
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
        var schema: Schema? = null
        try {
            val catalogueTxn2 = txn2.getTx(this.catalogue) as CatalogueTx
            schema = catalogueTxn2.schemaForName(this.schemaName)

            /* Check if schema directory exists. */
            Assertions.assertTrue(Files.isReadable(schema.path))
            Assertions.assertTrue(Files.isDirectory(schema.path))

            /* Drop schema. */
            catalogueTxn2.dropSchema(this.schemaName)
            txn2.commit()
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
            Assertions.assertFalse(Files.exists(schema.path))
        } finally {
            txn3.rollback()
        }
    }
}
