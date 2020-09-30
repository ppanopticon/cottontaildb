package org.vitrivr.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.FloatVectorValue
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer

/**
 * A [Serializer] for [FloatVectorValue]s that a fixed in length.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FixedFloatVectorSerializer(override val logicalSize: Int): Serializer<FloatVectorValue> {

    override val physicalSize: Int = (this.logicalSize shl 2) /* Equals (this.logicalSize * Int.SIZE_BYTES) */
    override fun serialize(out: DataOutput2, value: FloatVectorValue) {
        for (i in 0 until this.logicalSize) {
            out.writeFloat(value[i].value)
        }
    }
    override fun deserialize(input: DataInput2, available: Int): FloatVectorValue {
        val vector = FloatArray(this.logicalSize) {input.readFloat()}
        return FloatVectorValue(vector)
    }

    override fun serialize(page: Page, offset: Int, value: FloatVectorValue) {
        page.putFloats(offset, value.data)
    }

    override fun deserialize(page: Page, offset: Int): FloatVectorValue = FloatVectorValue(page.getFloats(offset, FloatArray(this.logicalSize)))
}