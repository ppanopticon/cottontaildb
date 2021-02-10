package org.vitrivr.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.model.values.DateValue

/**
 * A [Serializer] for [DateValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DateValueSerializer : Serializer<DateValue> {
    override fun deserialize(input: DataInput2, available: Int): DateValue =
        DateValue(input.readLong())

    override fun serialize(out: DataOutput2, value: DateValue) {
        out.writeLong(value.value)
    }

    override fun isTrusted(): Boolean = true
}