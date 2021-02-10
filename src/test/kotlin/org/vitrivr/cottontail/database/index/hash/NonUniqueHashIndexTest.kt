package org.vitrivr.cottontail.database.index.hash

import org.junit.jupiter.api.*
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.index.AbstractIndexTest
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.components.AtomicBooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.model.values.StringValue
import java.util.*
import kotlin.collections.HashMap

/**
 * This is a collection of test cases to test the correct behaviour of [UniqueHashIndex].
 *
 * @author Ralph Gasser
 * @param 1.2.0
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NonUniqueHashIndexTest : AbstractIndexTest() {

    /** List of columns for this [NonUniqueHashIndexTest]. */
    override val columns = arrayOf(
        ColumnDef.withAttributes(this.entityName.column("id"), "STRING", -1, false),
        ColumnDef.withAttributes(this.entityName.column("value"), "LONG", -1, false)
    )

    override val indexColumn: ColumnDef<*>
        get() = this.columns.first()

    override val indexName: Name.IndexName
        get() = this.entityName.index("idx_id_non-unique")

    override val indexType: IndexType
        get() = IndexType.HASH

    /** List of values stored in this [UniqueHashIndexTest]. */
    private var list = HashMap<StringValue, MutableList<LongValue>>(100)

    /** Random number generator. */
    private val random = SplittableRandom()

    @BeforeAll
    override fun initialize() {
        super.initialize()
    }

    @AfterAll
    override fun teardown() {
        super.teardown()
        this.list.clear()
    }

    /**
     * Tests basic metadata information regarding the [UniqueHashIndex]
     */
    @Test
    fun testMetadata() {
        Assertions.assertNotNull(this.index)
        Assertions.assertArrayEquals(arrayOf(this.columns[0]), this.index?.columns)
        Assertions.assertArrayEquals(arrayOf(this.columns[0]), this.index?.produces)
        Assertions.assertEquals(this.indexName, this.index?.name)
    }

    /**
     * Tests if Index#filter() returns the values that have been stored.
     */
    @RepeatedTest(3)
    fun testFilterEqualPositive() {
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        val indexTx = txn.getTx(this.index!!) as IndexTx
        val entityTx = txn.getTx(this.entity!!) as EntityTx
        for (entry in this.list.entries) {
            val predicate = AtomicBooleanPredicate(
                this.columns[0] as ColumnDef<StringValue>,
                ComparisonOperator.EQUAL,
                false,
                listOf(entry.key)
            )
            indexTx.filter(predicate).use {
                it.forEach { r ->
                    val rec = entityTx.read(r.tupleId, this.columns)
                    val id = rec[this.columns[0]] as StringValue
                    Assertions.assertEquals(entry.key, id)
                    Assertions.assertTrue(list[id]!!.contains(rec[this.columns[1]] as LongValue))
                }
            }
        }
        txn.commit()
    }

    /**
     * Tests if Index#filter() only returns stored values.
     */
    @RepeatedTest(3)
    fun testFilterEqualNegative() {
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        val indexTx = txn.getTx(this.index!!) as IndexTx
        var count = 0
        indexTx.filter(
            AtomicBooleanPredicate(
                this.columns[0] as ColumnDef<StringValue>,
                ComparisonOperator.EQUAL,
                false,
                listOf(StringValue(UUID.randomUUID().toString()))
            )
        ).use {
            it.forEach { count += 1 }
        }
        Assertions.assertEquals(0, count)
        txn.commit()
    }

    /**
     * Generates and returns a new, random [StandaloneRecord] for inserting into the database.
     */
    override fun nextRecord(): StandaloneRecord {
        val id = StringValue.random(3)
        val value = LongValue(random.nextLong())
        if (this.random.nextBoolean() && this.list.size <= 1000) {
            this.list.compute(id) { k, v ->
                val list = v ?: LinkedList()
                list.add(value)
                list
            }
        }
        return StandaloneRecord(columns = this.columns, values = arrayOf(id, value))
    }
}