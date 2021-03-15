package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.FloatVectorValue

/**
 * A [MapDBSerializer] for MapDB based [DoubleValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FloatVectorMapDBValueSerializer(val size: Int) : MapDBSerializer<FloatVectorValue> {
    init {
        require(this.size > 0) { "Cannot initialize vector value serializer with size value of $size." }
    }

    override fun deserialize(input: DataInput2, available: Int): FloatVectorValue = FloatVectorValue(FloatArray(this.size) { input.readFloat() })
    override fun serialize(out: DataOutput2, value: FloatVectorValue) {
        for (i in 0 until size) {
            out.writeFloat(value[i].value)
        }
    }
}