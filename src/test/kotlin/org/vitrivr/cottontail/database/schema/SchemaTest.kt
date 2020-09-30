package org.vitrivr.cottontail.database.schema

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import java.nio.file.Files
import java.util.*
import java.util.stream.Collectors

class SchemaTest {

    private val schemaName = Name.SchemaName("schema-test")

    /** */
    private var catalogue: Catalogue = Catalogue(TestConstants.config)

    private var schema: Schema? = null


    @BeforeEach
    fun initialize() {
        this.catalogue.createSchema(schemaName)
        this.schema = this.catalogue.schemaForName(schemaName)
    }

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
    fun EntityCreateTest() {
        /* Create a few entities. */
        val entityNames = arrayOf("one", "two", "three")
        for (name in entityNames) {
            schema?.createEntity(Name.EntityName("test", name), ColumnDef.withAttributes(Name.ColumnName("id"), "STRING"))
            assertTrue(Files.isReadable(TestConstants.config.root.resolve("schema_schema-test").resolve("entity_$name")))
            assertTrue(Files.isDirectory(TestConstants.config.root.resolve("schema_schema-test").resolve("entity_$name")))
            assertTrue(Files.isReadable(TestConstants.config.root.resolve("schema_schema-test").resolve("entity_$name").resolve("col_id.db")))
            assertTrue(Files.isReadable(TestConstants.config.root.resolve("schema_schema-test").resolve("entity_$name").resolve(Entity.FILE_CATALOGUE)))
        }

        /* Check size of the schema. */
        assertEquals(entityNames.size, schema?.size)

        /* Check stored entity names. */
        entityNames.zip(schema!!.entities) { a, b ->
            assertEquals(a, b.simple)
        }
    }
}
