package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.LongVectorValue
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

/**
 * A [Serializer] for [LongVectorValue]s that are fixed in length.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FixedLongVectorSerializer(val size: Int): Serializer<LongVectorValue> {
    override fun serialize(out: DataOutput2, value: LongVectorValue) {
        for (i in 0 until size) {
            out.writeLong(value[i].value)
        }
    }
    override fun deserialize(input: DataInput2, available: Int): LongVectorValue {
        val vector = LongArray(size)
        for (i in 0 until size) {
            vector[i] = input.readLong()
        }
        return LongVectorValue(vector)
    }
}