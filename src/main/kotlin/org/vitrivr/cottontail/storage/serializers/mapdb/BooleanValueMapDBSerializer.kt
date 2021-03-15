package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.BooleanValue

/**
 * A [MapDBSerializer] for MapDB based [BooleanValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object BooleanValueMapDBSerializer : MapDBSerializer<BooleanValue> {
    override fun deserialize(input: DataInput2, available: Int): BooleanValue = BooleanValue(input.readBoolean())
    override fun serialize(out: DataOutput2, value: BooleanValue) {
        out.writeBoolean(value.value)
    }
}