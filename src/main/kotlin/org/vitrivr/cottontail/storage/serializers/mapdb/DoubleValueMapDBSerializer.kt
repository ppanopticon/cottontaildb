package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.serializer.SerializerEightByte
import org.vitrivr.cottontail.model.values.DoubleValue
import java.util.*

/**
 * A [MapDBSerializer] for MapDB based [DoubleValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DoubleValueMapDBSerializer : MapDBSerializer<DoubleValue>, SerializerEightByte<DoubleValue>() {
    override fun deserialize(input: DataInput2, available: Int): DoubleValue = DoubleValue(input.readDouble())
    override fun serialize(out: DataOutput2, value: DoubleValue) {
        out.writeDouble(value.value)
    }

    override fun isTrusted(): Boolean = true
    override fun unpack(l: Long): DoubleValue = DoubleValue(Double.fromBits(l))
    override fun pack(l: DoubleValue): Long = l.value.toBits()
    override fun valueArraySearch(keys: Any, key: DoubleValue): Int {
        return Arrays.binarySearch(valueArrayToArray(keys), key)
    }
}