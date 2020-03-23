package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.DoubleValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel

object DoubleValueSerializer : Serializer<DoubleValue> {

    private val buffer = ByteBuffer.allocate(Long.SIZE_BYTES)
    override fun deserialize(input: DataInput2, available: Int): DoubleValue = DoubleValue(input.readDouble())
    override fun serialize(out: DataOutput2, value: DoubleValue) {
        out.writeDouble(value.value)
    }
    override val physicalSize: Int = Long.SIZE_BYTES
    override val logicalSize: Int = -1
    override fun serialize(channel: WritableByteChannel, value: DoubleValue) {
        this.buffer.putDouble(0, value.value)
        channel.write(this.buffer.rewind())
    }

    override fun deserialize(channel: ReadableByteChannel): DoubleValue {
        channel.read(this.buffer.rewind())
        return DoubleValue(this.buffer.getDouble(0))
    }
}