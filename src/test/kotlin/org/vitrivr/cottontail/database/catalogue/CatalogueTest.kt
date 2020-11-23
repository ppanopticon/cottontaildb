package org.vitrivr.cottontail.database.catalogue

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import java.nio.file.Files
import java.util.*
import java.util.stream.Collectors

class CatalogueTest {
    private val schemaName = Name.SchemaName("schema-test")

    /** The [Catalogue] used for the test. */
    private var catalogue: Catalogue = Catalogue(TestConstants.config)


    @AfterEach
    fun teardown() {
        this.catalogue.close()
        val pathsToDelete = Files.walk(TestConstants.config.root).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
        pathsToDelete.forEach { Files.delete(it) }
    }

    /**
     * Creates a new [Schema] and runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun createSchemaTest() {
        this.catalogue.createSchema(this.schemaName)
        val schema = this.catalogue.schemaForName(this.schemaName)

        /* Check if directory exists. */
        Assertions.assertTrue(Files.isReadable(schema.path))
        Assertions.assertTrue(Files.isDirectory(schema.path))

        /* Check if schema contains the expected number of entities (zero). */
        Assertions.assertEquals(0, schema.entities.size)
    }

    /**
     * Creates a new [Schema] and then drops it. Runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun dropSchemaTest() {
        this.catalogue.createSchema(schemaName)
        val schema = this.catalogue.schemaForName(this.schemaName)

        /* Check if directory exists. */
        Assertions.assertTrue(Files.isReadable(schema.path))
        Assertions.assertTrue(Files.isDirectory(schema.path))

        /* Now drop schema. */
        this.catalogue.dropSchema(schemaName)

        /* Check if directory exists. */
        Assertions.assertFalse(Files.isReadable(schema.path))
        Assertions.assertFalse(Files.isDirectory(schema.path))

        /* Check that correct exception is thrown. */
        Assertions.assertThrows(DatabaseException.SchemaDoesNotExistException::class.java) { catalogue.schemaForName(schemaName) }
    }
}
