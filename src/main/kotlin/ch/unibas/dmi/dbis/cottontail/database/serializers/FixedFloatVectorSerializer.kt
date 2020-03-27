package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.FloatVectorValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2

/**
 * A [Serializer] for [FloatVectorValue]s that a fixed in length.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FixedFloatVectorSerializer(override val logicalSize: Int): Serializer<FloatVectorValue> {
    override fun serialize(out: DataOutput2, value: FloatVectorValue) {
        for (i in 0 until this.logicalSize) {
            out.writeFloat(value[i].value)
        }
    }
    override fun deserialize(input: DataInput2, available: Int): FloatVectorValue {
        val vector = FloatArray(this.logicalSize) {input.readFloat()}
        return FloatVectorValue(vector)
    }

    override val physicalSize: Int = this.logicalSize * Int.Companion.SIZE_BYTES
    override fun serialize(page: Page, offset: Int, value: FloatVectorValue) {
        TODO("Not yet implemented")
    }

    override fun deserialize(page: Page, offset: Int): FloatVectorValue {
        TODO("Not yet implemented")
    }

}