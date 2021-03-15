package org.vitrivr.cottontail.database.index.hash

import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.index.AbstractIndexTest
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.FloatVectorValue
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.types.Value
import java.util.*
import kotlin.collections.HashMap

/**
 * This is a collection of test cases to test the correct behaviour of [UniqueHashIndex].
 *
 * @author Ralph Gasser
 * @param 1.2.0
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UniqueHashIndexTest : AbstractIndexTest() {

    /** List of columns for this [NonUniqueHashIndexTest]. */
    override val columns = arrayOf(
        ColumnDef(this.entityName.column("id"), Type.String),
        ColumnDef(this.entityName.column("feature"), Type.FloatVector(128))
    )
    override val indexColumn: ColumnDef<*>
        get() = this.columns.first()

    override val indexName: Name.IndexName
        get() = this.entityName.index("idx_id_unique")

    override val indexType: IndexType
        get() = IndexType.HASH_UQ

    /** List of values stored in this [UniqueHashIndexTest]. */
    private var list = HashMap<StringValue, FloatVectorValue>(100)

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
        assertNotNull(this.index)
        assertArrayEquals(arrayOf(this.columns[0]), this.index?.columns)
        assertArrayEquals(arrayOf(this.columns[0]), this.index?.produces)
        assertEquals(this.indexName, this.index?.name)
    }

    /**
     * Tests if Index#filter() returns the values that have been stored.
     */
    @RepeatedTest(3)
    fun testFilterEqualPositive() {
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        val indexTx = txn.getTx(this.index!!) as IndexTx
        val entityTx = txn.getTx(this.entity!!) as EntityTx
        val context = BindingContext<Value>()
        for (entry in this.list.entries) {
            val predicate = BooleanPredicate.Atomic.Literal(
                this.columns[0] as ColumnDef<StringValue>,
                ComparisonOperator.Binary.Equal(context.bind(entry.key)),
                false,
            )
            indexTx.filter(predicate).forEach { r ->
                val rec = entityTx.read(r.tupleId, this.columns)
                assertEquals(entry.key, rec[this.columns[0]])
                assertArrayEquals(
                    entry.value.data,
                    (rec[this.columns[1]] as FloatVectorValue).data
                )
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
        val context = BindingContext<Value>()
        val predicate = BooleanPredicate.Atomic.Literal(
            this.columns[0] as ColumnDef<StringValue>,
            ComparisonOperator.Binary.Equal(context.bind(StringValue(UUID.randomUUID().toString()))),
            false
        )
        indexTx.filter(predicate).forEach { count += 1 }
        assertEquals(0, count)
        txn.commit()
    }

    /**
     * Generates and returns a new, random [StandaloneRecord] for inserting into the database.
     */
    override fun nextRecord(): StandaloneRecord {
        val uuid = StringValue(UUID.randomUUID().toString())
        val vector = FloatVectorValue.random(128, random)
        if (this.random.nextBoolean() && this.list.size <= 1000) {
            this.list[uuid] = vector
        }
        return StandaloneRecord(0L, columns = this.columns, values = arrayOf(uuid, vector))
    }
}
