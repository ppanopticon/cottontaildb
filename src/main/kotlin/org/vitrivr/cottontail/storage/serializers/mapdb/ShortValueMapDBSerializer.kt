package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.ShortValue

/**
 * A [MapDBSerializer] for MapDB based [ShortValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object ShortValueMapDBSerializer : MapDBSerializer<ShortValue> {
    override fun deserialize(input: DataInput2, available: Int): ShortValue = ShortValue(input.readShort())
    override fun serialize(out: DataOutput2, value: ShortValue) {
        out.writeShort(value.value.toInt())
    }
}