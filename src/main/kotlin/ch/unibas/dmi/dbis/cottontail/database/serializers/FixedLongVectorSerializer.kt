package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.LongVectorValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel

/**
 * A [Serializer] for [LongVectorValue]s that are fixed in length.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FixedLongVectorSerializer(override val logicalSize: Int): Serializer<LongVectorValue> {
    override fun serialize(out: DataOutput2, value: LongVectorValue) {
        for (i in 0 until this.logicalSize) {
            out.writeLong(value[i].value)
        }
    }
    override fun deserialize(input: DataInput2, available: Int): LongVectorValue {
        val vector = LongArray(this.logicalSize)
        for (i in 0 until this.logicalSize) {
            vector[i] = input.readLong()
        }
        return LongVectorValue(vector)
    }

    override val physicalSize: Int = this.logicalSize * Long.Companion.SIZE_BYTES
    override fun serialize(channel: WritableByteChannel, value: LongVectorValue) {
        TODO("Not yet implemented")
    }

    override fun deserialize(channel: ReadableByteChannel): LongVectorValue {
        TODO("Not yet implemented")
    }
}