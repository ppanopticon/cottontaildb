package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2

import org.vitrivr.cottontail.model.values.LongVectorValue

/**
 * A [MapDBSerializer] for MapDB based [LongVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LongVectorValueMapDBSerializer(val size: Int) : MapDBSerializer<LongVectorValue> {
    init {
        require(this.size > 0) { "Cannot initialize vector value serializer with size value of $size." }
    }

    override fun deserialize(input: DataInput2, available: Int): LongVectorValue = LongVectorValue(LongArray(this.size) { input.readLong() })
    override fun serialize(out: DataOutput2, value: LongVectorValue) {
        for (i in 0 until size) {
            out.writeLong(value[i].value)
        }
    }
}