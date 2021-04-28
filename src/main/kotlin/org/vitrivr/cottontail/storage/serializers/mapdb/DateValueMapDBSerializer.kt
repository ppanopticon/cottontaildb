package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.serializer.SerializerEightByte
import org.vitrivr.cottontail.model.values.DateValue
import java.util.*

/**
 * A [MapDBSerializer] for MapDB based [DateValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object DateValueMapDBSerializer : MapDBSerializer<DateValue>, SerializerEightByte<DateValue>() {
    override fun deserialize(input: DataInput2, available: Int): DateValue = DateValue(input.readLong())
    override fun serialize(out: DataOutput2, value: DateValue) {
        out.writeLong(value.value)
    }

    override fun isTrusted(): Boolean = true
    override fun valueArraySearch(keys: Any, key: DateValue): Int = Arrays.binarySearch(keys as LongArray, key.value)
    override fun unpack(l: Long): DateValue = DateValue(l)
    override fun pack(l: DateValue): Long = l.value
    override fun valueArrayBinarySearch(key: DateValue, input: DataInput2, keysLen: Int, comparator: Comparator<*>): Int {
        if (comparator !== this) return super.valueArrayBinarySearch(key, input, keysLen, comparator)
        for (pos in 0 until keysLen) {
            val from = input.readLong()
            if (key.value <= from) {
                input.skipBytes((keysLen - pos - 1) * 8)
                return if (key.value == from) pos else -(pos + 1)
            }
        }
        return -(keysLen + 1)
    }
}