package ch.unibas.dmi.dbis.cottontail.database.data

import ch.unibas.dmi.dbis.cottontail.TestConstants
import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.model.recordset.StandaloneRecord
import ch.unibas.dmi.dbis.cottontail.model.values.*

import ch.unibas.dmi.dbis.cottontail.utilities.VectorUtility
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.RepeatedTest

import java.util.*
import kotlin.collections.HashMap
import kotlin.math.absoluteValue


import org.junit.jupiter.api.Assertions.*
import java.nio.file.Files
import java.util.stream.Collectors


class VectorDataTest {
    private val schemaName = Name("data-test")
    private val entityName = Name("vector-test")

    private val random = Random()

    /** */
    private val catalogue = Catalogue(TestConstants.config)

    private var schema: Schema? = null

    @BeforeEach
    fun initialize() {
        this.catalogue.createSchema(this.schemaName)
        this.schema = this.catalogue.schemaForName(this.schemaName)
    }

    @AfterEach
    fun teardown() {
        this.catalogue.close()
        val pathsToDelete = Files.walk(TestConstants.config.root).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
        pathsToDelete.forEach { Files.delete(it) }
    }

    /**
     * Tries to persist some random float vectors + Int field and tests, if the persisted version equals the original version.
     */
    @RepeatedTest(30)
    fun insertIntVectorTest() {
        val size = random.nextInt(1024).absoluteValue
        val count = 10000
        System.out.println("Running float Int test with d=$size.")
        val intField = ColumnDef.withAttributes(Name("counter"), "INTEGER", -1)
        val vectorField = ColumnDef.withAttributes(Name("vector"), "INT_VEC", size)

        schema?.createEntity(entityName, intField, vectorField)
        val entity = schema?.entityForName(entityName)!!

        /* Insert the int vectors. */
        val tx = entity.Tx(false)
        val iterator = VectorUtility.randomIntVectorSequence(size, count)
        val vectorMap = HashMap<Long,IntVectorValue>()
        val counterMap = HashMap<Long,Int>()
        var counter = 0
        val columns =arrayOf(intField, vectorField)
        tx.begin {
            iterator.forEach {
                val record = StandaloneRecord(columns = columns)
                record[columns[0]] = IntValue(counter)
                record[columns[1]] = it
                val tid = tx.insert(record)
                assertNotNull(tid)
                vectorMap[tid!!] = it
                counterMap[tid] = counter
                counter += 1
            }
            true
        }

        /* Fetch and compare the int vectors. */
        val tx2 = entity.Tx(readonly = true, columns = columns)
        assertEquals(count.toLong(), tx2.count())
        tx2.begin {
            vectorMap.forEach { (t, u) ->
                val tuple = tx2.read(t)
                assertEquals(u, tuple[vectorField]!!)
                assertEquals(counterMap[t], tuple[intField]!!)
            }
            true
        }
    }

    /**
     * Tries to persist some random float vectors + Int field and tests, if the persisted version equals the original version.
     */
    @RepeatedTest(30)
    fun insertLongVectorTest() {
        val size = random.nextInt(1024).absoluteValue
        val count = 10000
        System.out.println("Running float vector test with d=$size.")
        val intField = ColumnDef.withAttributes(Name("counter"), "INTEGER", -1)
        val vectorField = ColumnDef.withAttributes(Name("vector"), "LONG_VEC", size)

        schema?.createEntity(entityName, intField, vectorField)
        val entity = schema?.entityForName(entityName)!!

        /* Insert the long vectors. */
        val tx = entity.Tx(false)
        val iterator = VectorUtility.randomLongVectorSequence(size, count)
        val vectorMap = HashMap<Long,LongArray>()
        val counterMap = HashMap<Long,Int>()
        var counter = 0
        val columns = arrayOf(intField, vectorField)
        tx.begin {
            iterator.forEach {
                val record = StandaloneRecord(columns = columns)
                record[columns[0]] = IntValue(counter)
                record[columns[1]] = LongVectorValue(it)
                val tid = tx.insert(record)
                assertNotNull(tid)
                vectorMap[tid!!] = it
                counterMap[tid] = counter
                counter += 1
            }
            true
        }

        /* Fetch and compare the long vectors. */
        val tx2 = entity.Tx(readonly = true, columns = columns)
        assertEquals(count.toLong(), tx2.count())
        tx2.begin {
            vectorMap.forEach { t, u ->
                val tuple = tx2.read(t)
                assertEquals(u, tuple[vectorField]!!)
                assertEquals(counterMap[t], tuple[intField]!!)
            }
            true
        }
    }

    /**
     * Tries to persist some random float vectors + Int field and tests, if the persisted version equals the original version.
     */
    @RepeatedTest(30)
    fun insertFloatVectorTest() {
        val size = random.nextInt(1024).absoluteValue
        val count = 10000
        System.out.println("Running float vector test with d=$size.")
        val intField = ColumnDef.withAttributes(Name("counter"), "INTEGER", -1)
        val vectorField = ColumnDef.withAttributes(Name("vector"), "FLOAT_VEC", size)

        schema?.createEntity(entityName, intField, vectorField)
        val entity = schema?.entityForName(entityName)!!

        /* Insert the float vectors. */
        val tx = entity.Tx(false)
        val iterator = VectorUtility.randomFloatVectorSequence(size, count)
        val vectorMap = HashMap<Long,FloatVectorValue>()
        val counterMap = HashMap<Long,Int>()
        var counter = 0
        val columns = arrayOf(intField, vectorField)
        tx.begin {
            iterator.forEach {
                val record = StandaloneRecord(columns = columns)
                record[columns[0]] = IntValue(counter)
                record[columns[1]] = it
                val tid = tx.insert(record)
                assertNotNull(tid)
                vectorMap[tid!!] = it
                counterMap[tid] = counter
                counter += 1
            }
            true
        }

        /* Fetch and compare the float vectors. */
        val tx2 = entity.Tx(readonly = true, columns = columns)
        assertEquals(count.toLong(), tx2.count())
        tx2.begin {
            vectorMap.forEach { (t, u) ->
                val tuple = tx2.read(t)
                assertEquals(u, tuple[vectorField]!!)
                assertEquals(counterMap[t], tuple[intField]!!)
            }
            true
        }
    }

    /**
     * Tries to persist some random float vectors + Int field and tests, if the persisted version equals the original version.
     */
    @RepeatedTest(30)
    fun insertDoubleVectorTest() {
        val size = random.nextInt(1024).absoluteValue
        val count = 10000
        System.out.println("Running double vector test with d=$size.")
        val intField = ColumnDef.withAttributes(Name("counter"), "INTEGER", -1)
        val vectorField = ColumnDef.withAttributes(Name("vector"), "DOUBLE_VEC", size)

        schema?.createEntity(entityName, intField, vectorField)
        val entity = schema?.entityForName(entityName)!!

        /* Insert the double vectors. */
        val tx = entity.Tx(false)
        val iterator = VectorUtility.randomDoubleVectorSequence(size, count)
        val vectorMap = HashMap<Long,DoubleVectorValue>()
        val counterMap = HashMap<Long,Int>()
        var counter = 0
        val columns = arrayOf(intField, vectorField)
        tx.begin {
            iterator.forEach {
                val record = StandaloneRecord(columns = columns)
                record[columns[0]] = IntValue(counter)
                record[columns[1]] = it
                val tid = tx.insert(record)
                assertNotNull(tid)
                vectorMap[tid!!] = it
                counterMap[tid] = counter
                counter += 1
            }
            true
        }

        /* Fetch and compare the double vectors. */
        val tx2 = entity.Tx(readonly = true, columns = columns)
        assertEquals(count.toLong(), tx2.count())
        tx2.begin {
            vectorMap.forEach { (t, u) ->
                val tuple = tx2.read(t)
                assertEquals(u, tuple[vectorField]!!)
                assertEquals(counterMap[t], tuple[intField]!!)
            }
            true
        }
    }
}