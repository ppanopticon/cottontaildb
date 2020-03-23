package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.ShortValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel

object ShortValueSerializer : Serializer<ShortValue> {
    override fun deserialize(input: DataInput2, available: Int): ShortValue = ShortValue(input.readShort())
    override fun serialize(out: DataOutput2, value: ShortValue) {
        out.writeShort(value.value.toInt())
    }
    override val physicalSize: Int = Short.SIZE_BYTES
    override val logicalSize: Int = -1
    override fun serialize(channel: WritableByteChannel, value: ShortValue) {
        TODO("Not yet implemented")
    }

    override fun deserialize(channel: ReadableByteChannel): ShortValue {
        TODO("Not yet implemented")
    }
}