package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.BooleanVectorValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import java.util.*


class FixedBooleanVectorSerializer(override val logicalSize: Int): Serializer<BooleanVectorValue> {
    override val physicalSize: Int = ((this.logicalSize+63)/64) * Long.Companion.SIZE_BYTES

    override fun serialize(out: DataOutput2, value: BooleanVectorValue) {
        val words = value.value.toLongArray()
        for (element in words) {
            out.writeLong(element)
        }
    }

    override fun deserialize(input: DataInput2, available: Int): BooleanVectorValue {
        val words = LongArray((this.logicalSize+63)/64)
        for (i in words.indices) {
            words[i] = input.readLong()
        }
        return BooleanVectorValue(BitSet.valueOf(words))
    }

    override fun serialize(page: Page, offset: Int, value: BooleanVectorValue) {
        TODO("Not yet implemented")
    }

    override fun deserialize(page: Page, offset: Int): BooleanVectorValue {
        TODO("Not yet implemented")
    }
}