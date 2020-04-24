package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.BooleanValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2

object BooleanValueSerializer : Serializer<BooleanValue> {
    private const val TRUE = 1.toByte()
    private const val FALSE = 0.toByte()

    override val physicalSize: Int = Byte.SIZE_BYTES
    override val logicalSize: Int = -1

    override fun deserialize(input: DataInput2, available: Int): BooleanValue = BooleanValue(input.readBoolean())
    override fun serialize(out: DataOutput2, value: BooleanValue) {
        out.writeBoolean(value.value)
    }

    override fun serialize(page: Page, offset: Int, value: BooleanValue) {
        page.putByte(offset, if (value.value) { TRUE } else { FALSE })
    }

    override fun deserialize(page: Page, offset: Int): BooleanValue = BooleanValue(page.getByte(offset) == TRUE)
}