package org.vitrivr.cottontail.database.index.lucene

import org.junit.jupiter.api.*
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.index.AbstractIndexTest
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.model.values.StringValue
import java.util.*

/**
 * A set of unit tests to test basic data insert functionality.
 *
 * @author Ralph Gasser & Silvan Heller
 * @version 1.2.0
 */
class LuceneIndexText : AbstractIndexTest() {
    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef.withAttributes(this.entityName.column("id"), "LONG"),
        ColumnDef.withAttributes(this.entityName.column("feature"), "STRING")
    )

    override val indexColumn: ColumnDef<*>
        get() = this.columns[1]

    override val indexName: Name.IndexName
        get() = this.entityName.index("idx_feature_lucene")

    override val indexType: IndexType
        get() = IndexType.LUCENE

    /** */
    private var id = 0L

    /** Random number generator. */
    private val random = SplittableRandom()

    @BeforeAll
    override fun initialize() {
        super.initialize()
    }

    @AfterAll
    override fun teardown() {
        super.teardown()
    }

    @Test
    fun testLuceneIndex() {

    }

    /**
     * Generates and returns a new, random [StandaloneRecord] for inserting into the database.
     */
    override fun nextRecord(): StandaloneRecord {
        val id = LongValue(this.id++)
        val value = StringValue.random(128, this.random)
        return StandaloneRecord(columns = this.columns, values = arrayOf(id, value))
    }
}
