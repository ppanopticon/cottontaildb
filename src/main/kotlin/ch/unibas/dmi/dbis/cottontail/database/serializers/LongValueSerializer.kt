package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.LongValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2

object LongValueSerializer : Serializer<LongValue> {
    override fun deserialize(input: DataInput2, available: Int): LongValue = LongValue(input.readLong())
    override fun serialize(out: DataOutput2, value: LongValue) {
        out.writeLong(value.value)
    }
    override val physicalSize: Int = Long.SIZE_BYTES
    override val logicalSize: Int = -1
    override fun serialize(page: Page, offset: Int, value: LongValue) {
        TODO("Not yet implemented")
    }

    override fun deserialize(page: Page, offset: Int): LongValue {
        TODO("Not yet implemented")
    }

}