package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.IntValue

/**
 * A [MapDBSerializer] for MapDB based [IntValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object IntValueMapDBSerializer : MapDBSerializer<IntValue> {
    override fun deserialize(input: DataInput2, available: Int): IntValue = IntValue(input.readInt())
    override fun serialize(out: DataOutput2, value: IntValue) {
        out.writeInt(value.value)
    }
}