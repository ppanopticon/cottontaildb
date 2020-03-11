package ch.unibas.dmi.dbis.cottontail.database.schema

import ch.unibas.dmi.dbis.cottontail.TestConstants
import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import org.junit.jupiter.api.AfterEach
import java.nio.file.Files

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import java.util.Comparator
import java.util.stream.Collectors

class SchemaTest {

    private val schemaName = Name("schema-test")

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
        val entityNames = arrayOf(Name("test1"), Name("test2"), Name("test3"))
        for (name in entityNames) {
            schema?.createEntity(name, ColumnDef.withAttributes(Name("id"), "STRING"))
            assertTrue(Files.isReadable(TestConstants.config.root.resolve("schema_${schemaName}").resolve("entity_$name")))
            assertTrue(Files.isDirectory(TestConstants.config.root.resolve("schema_${schemaName}").resolve("entity_$name")))
            assertTrue(Files.isReadable(TestConstants.config.root.resolve("schema_${schemaName}").resolve("entity_$name").resolve("col_id.db")))
            assertTrue(Files.isReadable(TestConstants.config.root.resolve("schema_${schemaName}").resolve("entity_$name").resolve(Entity.FILE_CATALOGUE)))
        }

        /* Check size of the schema. */
        assertEquals(entityNames.size, schema?.size)

        /* Check stored entity names. */
        entityNames.zip(schema!!.entities) { a, b ->
            assertEquals(a,b)
        }
    }
}