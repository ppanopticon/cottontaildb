package ch.unibas.dmi.dbis.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer

class FixedStringValueSerializer(override val logicalSize: Int) : Serializer<StringValue> {

    companion object {
        const val NUL = 0.toChar()
    }
    override val physicalSize: Int = (this.logicalSize shl 1)

    override fun deserialize(input: DataInput2, available: Int): StringValue = StringValue(input.readUTF())
    override fun serialize(out: DataOutput2, value: StringValue) {
        out.writeUTF(value.value)
    }

    override fun serialize(page: Page, offset: Int, value: StringValue) {
        for (i in offset until offset + value.logicalSize.coerceAtMost(this.logicalSize)) {
            page.putChar(i, value.value[i])
        }
        for (i in offset + value.logicalSize until offset + this.logicalSize) {
            page.putChar(i, NUL)
        }
    }

    override fun deserialize(page: Page, offset: Int): StringValue = StringValue(String(page.getChars(offset, CharArray(this.logicalSize))))
}