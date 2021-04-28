package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.serializer.SerializerFourByte
import org.vitrivr.cottontail.model.values.FloatValue
import java.util.*

/**
 * A [MapDBSerializer] for MapDB based [FloatValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object FloatValueMapDBSerializer : MapDBSerializer<FloatValue>, SerializerFourByte<FloatValue>() {
    override fun deserialize(input: DataInput2, available: Int): FloatValue = FloatValue(input.readFloat())
    override fun serialize(out: DataOutput2, value: FloatValue) {
        out.writeFloat(value.value)
    }

    override fun isTrusted(): Boolean = true
    override fun unpack(l: Int): FloatValue = FloatValue(Float.fromBits(l))
    override fun pack(l: FloatValue): Int = l.value.toBits()
    override fun valueArraySearch(keys: Any, key: FloatValue): Int {
        return Arrays.binarySearch(valueArrayToArray(keys), key)
    }
}