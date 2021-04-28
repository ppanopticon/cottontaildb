package org.vitrivr.cottontail.database.index.hash

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.index.AbstractIndexTest
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.types.Value
import java.util.*
import kotlin.collections.HashMap

/**
 * This is a collection of test cases to test the correct behaviour of [UniqueHashIndex] with a [StringValue].
 *
 * @author Ralph Gasser
 * @param 1.2.0
 */
class NonUniqueStringHashIndexTest : AbstractIndexTest() {

    /** List of columns for this [NonUniqueStringHashIndexTest]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Type.String),
        ColumnDef(this.entityName.column("feature"), Type.Long)
    )

    override val indexColumn: ColumnDef<*>
        get() = this.columns.first()

    override val indexName: Name.IndexName
        get() = this.entityName.index("non_unique_string")

    override val indexType: IndexType
        get() = IndexType.HASH

    /** List of values stored in this [UniqueHashIndexTest]. */
    private var list = HashMap<StringValue, MutableList<LongValue>>(100)

    /** Random number generator. */
    private val random = SplittableRandom()

    /**
     * Tests if Index#filter() returns the values that have been stored.
     */
    @RepeatedTest(3)
    fun testFilterEqualPositive() {
        /* Obtain necessary transactions. */
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = txn.getTx(schema) as SchemaTx
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = txn.getTx(entity) as EntityTx
        val index = entityTx.indexForName(this.indexName)
        val indexTx = txn.getTx(index) as IndexTx

        val context = BindingContext<Value>()
        for (entry in this.list.entries) {
            val predicate = BooleanPredicate.Atomic.Literal(
                    this.columns[0] as ColumnDef<StringValue>,
                    ComparisonOperator.Binary.Equal(context.bind(entry.key)),
                    false,
            )
            var found = false
            indexTx.filter(predicate).forEach { r ->
                val rec = entityTx.read(r.tupleId, this.columns)
                val id = rec[this.columns[0]] as StringValue
                Assertions.assertEquals(entry.key, id)
                if (entry.value.contains(rec[this.columns[1]])) {
                    found = true
                }
            }
            Assertions.assertTrue(found)
        }
        txn.commit()
    }

    /**
     * Tests if Index#filter() only returns stored values.
     */
    @RepeatedTest(3)
    fun testFilterEqualNegative() {
        /* Obtain necessary transactions. */
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = txn.getTx(schema) as SchemaTx
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = txn.getTx(entity) as EntityTx
        val index = entityTx.indexForName(this.indexName)
        val indexTx = txn.getTx(index) as IndexTx

        var count = 0
        val context = BindingContext<Value>()
        val predicate = BooleanPredicate.Atomic.Literal(
                this.columns[0] as ColumnDef<StringValue>,
                ComparisonOperator.Binary.Equal(context.bind(StringValue(UUID.randomUUID().toString()))),
                false
        )
        indexTx.filter(predicate).forEach { count += 1 }
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
        return StandaloneRecord(0L, columns = this.columns, values = arrayOf(id, value))
    }
}