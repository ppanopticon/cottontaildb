package org.vitrivr.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer

/**
 * A [Serializer] for [DoubleVectorValue]s that a fixed in length.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FixedDoubleVectorSerializer(override val logicalSize: Int): Serializer<DoubleVectorValue> {

    override val physicalSize: Int = (this.logicalSize shl 3) /* Equals (this.logicalSize * Long.SIZE_BYTES) */

    override fun serialize(out: DataOutput2, value: DoubleVectorValue) {
        for (i in 0 until this.logicalSize) {
            out.writeDouble(value[i].value)
        }
    }

    override fun deserialize(input: DataInput2, available: Int): DoubleVectorValue {
        val vector = DoubleArray(this.logicalSize)
        for (i in 0 until this.logicalSize) {
            vector[i] = input.readDouble()
        }
        return DoubleVectorValue(vector)
    }

    override fun serialize(page: Page, offset: Int, value: DoubleVectorValue) {
        page.putDoubles(offset, value.data)
    }

    override fun deserialize(page: Page, offset: Int): DoubleVectorValue = DoubleVectorValue(page.getDoubles(offset, DoubleArray(this.logicalSize)))
}