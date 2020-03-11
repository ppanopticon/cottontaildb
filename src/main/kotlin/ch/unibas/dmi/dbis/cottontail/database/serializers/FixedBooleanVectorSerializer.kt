package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.BooleanVectorValue
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import java.util.*


class FixedBooleanVectorSerializer(val size: Int): Serializer<BooleanVectorValue> {
    override fun serialize(out: DataOutput2, value: BooleanVectorValue) {
        val words = value.value.toLongArray()
        for (element in words) {
            out.writeLong(element)
        }
    }

    override fun deserialize(input: DataInput2, available: Int): BooleanVectorValue {
        val words = LongArray((size+63)/64)
        for (i in words.indices) {
            words[i] = input.readLong()
        }
        return BooleanVectorValue(BitSet.valueOf(words))
    }
}