package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.DoubleValue

/**
 * A [MapDBSerializer] for MapDB based [DoubleValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DoubleValueMapDBSerializer : MapDBSerializer<DoubleValue> {
    override fun deserialize(input: DataInput2, available: Int): DoubleValue = DoubleValue(input.readDouble())
    override fun serialize(out: DataOutput2, value: DoubleValue) {
        out.writeDouble(value.value)
    }
}