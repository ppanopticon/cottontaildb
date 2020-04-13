package ch.unibas.dmi.dbis.cottontail.database.index
/*
import ch.unibas.dmi.dbis.cottontail.TestConstants
import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.database.queries.AtomicBooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.ComparisonOperator
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.recordset.StandaloneRecord
import ch.unibas.dmi.dbis.cottontail.model.values.FloatVectorValue
import ch.unibas.dmi.dbis.cottontail.model.values.StringValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.utilities.VectorUtility
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name

import org.junit.jupiter.api.*

import org.junit.jupiter.api.Assertions.*

import java.nio.file.Files
import java.util.*
import java.util.stream.Collectors

/**
 * This is a collection of test cases to test the correct behaviour of [UniqueHashIndex].
 *
 * @author Ralph Gasser
 * @param 1.0
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UniqueHashIndexTest {

    private val collectionSize = 1_000_000
    private val schemaName = Name("schema-test")
    private val entityName = Name("entity-test")
    private val indexName = Name("${this.entityName}_id_hash_index")

    private val columns = arrayOf(
            ColumnDef.withAttributes(Name("id"), "STRING", -1, false),
            ColumnDef.withAttributes(Name("feature"), "FLOAT_VEC", 128, false)
    )

    /** Catalogue used for testing. */
    private var catalogue: Catalogue = Catalogue(TestConstants.config)

    /** Schema used for testing. */
    private var schema: Schema? = null

    /** Schema used for testing. */
    private var entity: Entity? = null

    /** Schema used for testing. */
    private var index: Index? = null

    /** List of values stored in this [UniqueHashIndexTest]. */
    private var list: MutableMap<StringValue, FloatVectorValue> = mutableMapOf()

    @BeforeAll
    fun initialize() {
        /* Create schema. */
        this.catalogue.createSchema(schemaName)
        this.schema = this.catalogue.schemaForName(schemaName)

        /* Create entity. */
        this.schema?.createEntity(this.entityName, *this.columns)
        this.entity = this.schema?.entityForName(this.entityName)

        /* Create index. */
        this.entity?.createIndex(indexName, IndexType.HASH_UQ, arrayOf(this.columns[0]))
        this.index = entity?.allIndexes()?.find { it.name == indexName }

        /* Populates the database with test values. */
        this.populateDatabase()
    }

    @AfterAll
    fun teardown() {
        this.catalogue.dropSchema(schemaName)
        this.catalogue.close()
        val pathsToDelete = Files.walk(TestConstants.config.root).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
        pathsToDelete.forEach { Files.delete(it) }
    }

    /**
     * Tests basic metadata information regarding the [UniqueHashIndex]
     */
    @Test
    fun testMetadata() {
        assertNotNull(this.index)
        assertArrayEquals(arrayOf(this.columns[0]), this.index?.columns)
        assertArrayEquals(arrayOf(this.columns[0]), this.index?.produces)
        assertEquals(this.indexName, this.index?.name)
    }

    /**
     * Tests if Index#filter() returns the values that have been stored.
     */
    @Test
    @RepeatedTest(100)
    fun testFilterEqualPositive() {
        this.entity?.Tx(readonly = true, columns = this.columns)?.begin { tx ->
            val entry = this.list.entries.random()
            val predicate = AtomicBooleanPredicate(this.columns[0] as ColumnDef<StringValue>, ComparisonOperator.EQUAL, false, listOf(entry.key))
            val index = tx.indexes().first()
            val rec = index.filter(predicate)
            assertEquals(1, rec.rowCount)
            assertEquals(1, rec.columnCount)
            assertEquals(entry.key, tx.read(rec.first()!!.tupleId)[this.columns[0]])
            assertEquals(entry.value, tx.read(rec.first()!!.tupleId)[this.columns[1]]!!)
            true
        }
    }

    /**
     * Tests if Index#filter() only returns stored values.
     */
    @Test
    @RepeatedTest(100)
    fun testFilterEqualNegative() {
        this.entity?.Tx(readonly = true, columns = this.columns)?.begin { tx ->
            val index = tx.indexes().first()
            val rec = index.filter(AtomicBooleanPredicate(this.columns[0] as ColumnDef<StringValue>, ComparisonOperator.EQUAL, false, listOf(StringValue(UUID.randomUUID().toString()))))
            assertEquals(0, rec.rowCount)
            assertEquals(1, rec.columnCount)
            true
        }
    }

    /**
     * Populates the test database with data.
     */
    private fun populateDatabase() {
        this.entity?.Tx(readonly = false, columns = this.columns)?.begin { tx ->
            for (i in 0..this.collectionSize) {
                val uuid = StringValue(UUID.randomUUID().toString())
                val vector = VectorUtility.randomFloatVector(128)
                val values: Array<Value?> = arrayOf(uuid, vector)
                this.list[uuid] = vector
                tx.insert(StandaloneRecord(columns = this.columns, init = values))
            }
            true
        }
        this.entity?.updateAllIndexes()
    }
}*/