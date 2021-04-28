package org.vitrivr.cottontail.storage.serialization

import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnEngine
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.model.values.LongVectorValue
import org.vitrivr.cottontail.model.values.StringValue
import java.util.*

/**
 * Test case that tests for correctness of [LongVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class LongVectorValueSerializationTest : AbstractSerializationTest() {

    /** Create a random vector between 2 and 2048 dimensions. */
    private val d = SplittableRandom().nextInt(2, TestConstants.largeVectorMaxDimension)

    /** Columns tested by this [LongVectorValueSerializationTest]. */
    override val columns: Array<Pair<ColumnDef<*>, ColumnEngine>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Type.Int) to ColumnEngine.MAPDB,
        ColumnDef(this.entityName.column("vector"), Type.LongVector(this.d)) to ColumnEngine.MAPDB
    )

    /** Name of this [LongVectorValueSerializationTest]. */
    override val name: String = "IntVectorSerialization($d)"

    /**
     * Generates the next [StandaloneRecord] and returns it.
     */
    override fun nextRecord(i: Int): StandaloneRecord {
        val id = IntValue(i)
        val vector = LongVectorValue.random(this.d, this.random)
        return StandaloneRecord(0L, columns = this.columns.map { it.first }.toTypedArray(), values = arrayOf(id, vector))
    }
}