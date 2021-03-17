package org.vitrivr.cottontail.database.catalogue

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnEngine
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import java.nio.file.Files
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.stream.Collectors

/**
 * A set of unit tests to test basic [Schema] functionality.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class SchemaTest {
    /** [Name.SchemaName] of the test schema. */
    private val schemaName = Name.SchemaName("test")

    /** List of [DefaultEntity] to create. */
    private val entityNames = arrayOf(
        this.schemaName.entity("one"),
        this.schemaName.entity("two"),
        this.schemaName.entity("three")
    )

    /** The [TransactionManager] used for this [CatalogueTest] instance. */
    private val manager = TransactionManager(Executors.newFixedThreadPool(1) as ThreadPoolExecutor)

    init {
        if (!Files.exists(TestConstants.config.root)) {
            Files.createDirectories(TestConstants.config.root)
        }
    }

    /** The [DefaultCatalogue] object to run the test with. */
    private val catalogue: DefaultCatalogue = DefaultCatalogue(TestConstants.config)

    /** The [Schema] object to run the test with. */
    private var schema: Schema? = null

    @BeforeEach
    fun initialize() {
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
            this.schema = catalogueTx.createSchema(this.schemaName)
        } finally {
            txn.commit()
        }
    }


    @AfterEach
    fun teardown() {
        this.schema?.close()
        this.catalogue.close()
        val pathsToDelete = Files.walk(TestConstants.config.root).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
        pathsToDelete.forEach { Files.delete(it) }
    }

    /**
     * Creates a new [Schema] and runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun createEntityWithCommitTest() {
        /* Create a few entities. */
        val entityNames = arrayOf(this.schemaName.entity("one"), this.schemaName.entity("two"), this.schemaName.entity("three"))

        /* Transaction 1: Create entity. */
        val txn1 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val schemaTx1 = txn1.getTx(this.schema!!) as SchemaTx
            for (name in entityNames) {
                schemaTx1.createEntity(name, ColumnDef(name.column("id"), Type.String) to ColumnEngine.MAPDB)
            }
            txn1.commit()
        } catch (t: Throwable) {
            txn1.rollback()
            throw t
        }

        /* Transaction 2: Query. */
        val txn2 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val schemaTx2 = txn2.getTx(this.schema!!) as SchemaTx
            for (name in entityNames) {
                val entity = schemaTx2.entityForName(name)
                assertTrue(Files.isReadable(entity.path))
                assertTrue(Files.isDirectory(entity.path))
            }

            /* Check size and content of schema. */
            val fetchedEntities = schemaTx2.listEntities()
            assertEquals(entityNames.size, fetchedEntities.size)
            assertTrue(fetchedEntities.all { entityNames.contains(it.name) })
        } finally {
            txn2.rollback()
        }
    }

    /**
     * Creates a new [Schema] and runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun createEntityWithRollbackTest() {
        /* Transaction 1: Create entity. */
        val txn1 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val schemaTx1 = txn1.getTx(this.schema!!) as SchemaTx
            for (name in entityNames) {
                schemaTx1.createEntity(name, ColumnDef(name.column("id"), Type.String) to ColumnEngine.MAPDB)
            }
        } finally {
            txn1.rollback()
        }

        /* Transaction 2: Query. */
        val txn2 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val schemaTx2 = txn2.getTx(this.schema!!) as SchemaTx
            for (name in entityNames) {
                Assertions.assertThrows(DatabaseException.EntityDoesNotExistException::class.java) {
                    schemaTx2.entityForName(name)
                }
            }
        } finally {
            txn2.rollback()
        }
    }
}
