package org.vitrivr.cottontail.database.catalogue

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import java.nio.file.Files
import java.util.*
import java.util.stream.Collectors

class CatalogueTest {
    private val schemaName = Name.SchemaName("schema-test")

    /** */
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
    fun CreateSchemaTest() {
        catalogue.createSchema(schemaName)
        val schema = catalogue.schemaForName(schemaName)

        /* Check if directory exists. */
        Assertions.assertTrue(Files.isReadable(TestConstants.config.root.resolve("schema_schema-test")))
        Assertions.assertTrue(Files.isDirectory(TestConstants.config.root.resolve("schema_schema-test")))

        /* Check if catalogue file exists. */
        Assertions.assertTrue(Files.isReadable(TestConstants.config.root.resolve("schema_schema-test").resolve(Schema.FILE_CATALOGUE)))
        Assertions.assertFalse(Files.isDirectory(TestConstants.config.root.resolve("schema_schema-test").resolve(Schema.FILE_CATALOGUE)))

        /* Check if schema contains the expected number of entities (zero). */
        Assertions.assertEquals(0, schema.size)
    }

    /**
     * Creates a new [Schema] and then drops it. Runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun DropSchemaTest() {
        catalogue.createSchema(schemaName)

        /* Check if directory exists. */
        Assertions.assertTrue(Files.isReadable(TestConstants.config.root.resolve("schema_schema-test")))
        Assertions.assertTrue(Files.isDirectory(TestConstants.config.root.resolve("schema_schema-test")))

        /* Check if catalogue file exists. */
        Assertions.assertTrue(Files.isReadable(TestConstants.config.root.resolve("schema_schema-test").resolve(Schema.FILE_CATALOGUE)))
        Assertions.assertFalse(Files.isDirectory(TestConstants.config.root.resolve("schema_schema-test").resolve(Schema.FILE_CATALOGUE)))

        /* Now drop schema. */
        catalogue.dropSchema(schemaName)

        /* Check if directory exists. */
        Assertions.assertFalse(Files.isReadable(TestConstants.config.root.resolve("schema_schema-test")))
        Assertions.assertFalse(Files.isReadable(TestConstants.config.root.resolve("schema_schema-test").resolve(Schema.FILE_CATALOGUE)))

        /* Check that correct exception is thrown. */
        Assertions.assertThrows(DatabaseException.SchemaDoesNotExistException::class.java) { catalogue.schemaForName(schemaName) }
    }

}
