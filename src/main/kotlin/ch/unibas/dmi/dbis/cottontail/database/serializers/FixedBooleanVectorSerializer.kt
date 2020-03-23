package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.BooleanVectorValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel
import java.util.*


class FixedBooleanVectorSerializer(override val logicalSize: Int): Serializer<BooleanVectorValue> {
    override fun serialize(out: DataOutput2, value: BooleanVectorValue) {
        val words = value.value.toLongArray()
        for (element in words) {
            out.writeLong(element)
        }
    }

    override fun deserialize(input: DataInput2, available: Int): BooleanVectorValue {
        val words = LongArray((this.logicalSize+63)/64)
        for (i in words.indices) {
            words[i] = input.readLong()
        }
        return BooleanVectorValue(BitSet.valueOf(words))
    }

    override val physicalSize: Int = ((this.logicalSize+63)/64) * kotlin.Long.Companion.SIZE_BYTES
    override fun serialize(channel: WritableByteChannel, value: BooleanVectorValue) {
        TODO("Not yet implemented")
    }

    override fun deserialize(channel: ReadableByteChannel): BooleanVectorValue {
        TODO("Not yet implemented")
    }
}