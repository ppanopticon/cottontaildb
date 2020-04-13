package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.DoubleVectorValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2

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
        for ((i,d) in value.data.withIndex()) {
            page.putDouble(offset + (i shl 3), d)
        }
    }

    override fun deserialize(page: Page, offset: Int): DoubleVectorValue {
        val slice = page.getSlice(offset, offset + this.physicalSize)
        return DoubleVectorValue(DoubleArray(this.logicalSize) {
            slice.double
        })
    }
}