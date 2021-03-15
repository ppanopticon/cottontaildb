package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.IntVectorValue

/**
 * A [MapDBSerializer] for MapDB based [IntVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IntVectorValueMapDBSerializer(val size: Int) : MapDBSerializer<IntVectorValue> {
    init {
        require(this.size > 0) { "Cannot initialize vector value serializer with size value of $size." }
    }

    override fun deserialize(input: DataInput2, available: Int): IntVectorValue = IntVectorValue(IntArray(this.size) { input.readInt() })
    override fun serialize(out: DataOutput2, value: IntVectorValue) {
        for (i in 0 until size) {
            out.writeInt(value[i].value)
        }
    }
}