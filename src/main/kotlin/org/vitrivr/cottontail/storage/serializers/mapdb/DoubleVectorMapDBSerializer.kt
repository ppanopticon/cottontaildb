package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.DoubleVectorValue

/**
 * A [MapDBSerializer] for MapDB based [DoubleVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DoubleVectorMapDBSerializer(val size: Int) : MapDBSerializer<DoubleVectorValue> {
    init {
        require(this.size > 0) { "Cannot initialize vector value serializer with size value of $size." }
    }

    override fun serialize(out: DataOutput2, value: DoubleVectorValue) {
        for (i in 0 until this.size) {
            out.writeDouble(value[i].value)
        }
    }

    override fun deserialize(input: DataInput2, available: Int): DoubleVectorValue = DoubleVectorValue(DoubleArray(this.size) {
        input.readDouble()
    })
}