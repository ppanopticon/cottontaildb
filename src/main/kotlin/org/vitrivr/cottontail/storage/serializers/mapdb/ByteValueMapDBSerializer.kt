package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.serializer.GroupSerializerObjectArray
import org.vitrivr.cottontail.model.values.ByteValue

/**
 * A [MapDBSerializer] for MapDB based [ByteValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object ByteValueMapDBSerializer : MapDBSerializer<ByteValue>, GroupSerializerObjectArray<ByteValue>() {
    override fun deserialize(input: DataInput2, available: Int): ByteValue = ByteValue(input.readByte())
    override fun serialize(out: DataOutput2, value: ByteValue) {
        out.writeByte(value.value.toInt())
    }

    override fun fixedSize(): Int = 1
    override fun isTrusted(): Boolean = true
}