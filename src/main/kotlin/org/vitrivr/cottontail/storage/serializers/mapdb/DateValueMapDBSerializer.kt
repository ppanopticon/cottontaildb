package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.DateValue

/**
 * A [MapDBSerializer] for MapDB based [DateValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DateValueMapDBSerializer : MapDBSerializer<DateValue> {
    override fun deserialize(input: DataInput2, available: Int): DateValue = DateValue(input.readLong())
    override fun serialize(out: DataOutput2, value: DateValue) {
        out.writeLong(value.value)
    }
}