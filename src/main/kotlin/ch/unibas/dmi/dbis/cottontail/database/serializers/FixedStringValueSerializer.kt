package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.StringValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel

class FixedStringValueSerializer(override val logicalSize: Int) : Serializer<StringValue> {
    override fun deserialize(input: DataInput2, available: Int): StringValue = StringValue(input.readUTF())
    override fun serialize(out: DataOutput2, value: StringValue) {
        out.writeUTF(value.value)
    }
    override val physicalSize: Int = this.logicalSize * Int.Companion.SIZE_BYTES
    override fun serialize(channel: WritableByteChannel, value: StringValue) {
        TODO("Not yet implemented")
    }

    override fun deserialize(channel: ReadableByteChannel): StringValue {
        TODO("Not yet implemented")
    }
}