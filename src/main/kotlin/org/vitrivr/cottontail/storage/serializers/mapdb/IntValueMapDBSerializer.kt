package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.serializer.SerializerFourByte
import org.vitrivr.cottontail.model.values.IntValue
import java.util.*

/**
 * A [MapDBSerializer] for MapDB based [IntValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object IntValueMapDBSerializer : MapDBSerializer<IntValue>, SerializerFourByte<IntValue>() {
    override fun deserialize(input: DataInput2, available: Int): IntValue = IntValue(input.readInt())
    override fun serialize(out: DataOutput2, value: IntValue) {
        out.writeInt(value.value)
    }

    override fun isTrusted(): Boolean = true
    override fun valueArraySearch(keys: Any, key: IntValue): Int = Arrays.binarySearch((keys as IntArray), key.value)
    override fun unpack(l: Int): IntValue = IntValue(l)
    override fun pack(l: IntValue): Int = l.value
    override fun valueArrayBinarySearch(key: IntValue, input: DataInput2, keysLen: Int, comparator: Comparator<*>): Int {
        if (comparator !== this) return super.valueArrayBinarySearch(key, input, keysLen, comparator)
        for (pos in 0 until keysLen) {
            val from = input.readInt()
            if (key.value <= from) {
                input.skipBytes((keysLen - pos - 1) * 4)
                return if (key.value == from) pos else -(pos + 1)
            }
        }
        return -(keysLen + 1)
    }
}