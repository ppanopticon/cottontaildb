package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.DoubleVectorValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel

/**
 * A [Serializer] for [DoubleVectorValue]s that a fixed in length.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FixedDoubleVectorSerializer(override val logicalSize: Int): Serializer<DoubleVectorValue> {
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

    override val physicalSize: Int = this.logicalSize * Long.Companion.SIZE_BYTES
    override fun serialize(channel: WritableByteChannel, value: DoubleVectorValue) {
        TODO("Not yet implemented")
    }

    override fun deserialize(channel: ReadableByteChannel): DoubleVectorValue {
        TODO("Not yet implemented")
    }

}