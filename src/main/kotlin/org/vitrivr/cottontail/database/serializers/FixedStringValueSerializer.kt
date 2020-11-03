package org.vitrivr.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer

/**
 * Serializes a [StringValue] as fixed-length string. Fixed-length strings occupy a fixed (maximum)
 * length. The end of the actual value is indicated by a NUL character.
 *
 * @version 1.0.0
 * @author Ralph Gasser
 */
class FixedStringValueSerializer(override val logicalSize: Int) : Serializer<StringValue> {

    companion object {
        const val NUL: Char = Char.MIN_VALUE
    }

    /** A [ThreadLocal], reusable [CharArray] that acts as buffer for this [FixedStringValueSerializer]. */
    private val buffer = ThreadLocal.withInitial { CharArray(this.logicalSize) }

    /** Physical size if a [StringValue] entry in bytes. */
    override val physicalSize: Int = this.logicalSize * Char.SIZE_BYTES /* Size of a UTF-8 string. */

    override fun deserialize(input: DataInput2, available: Int): StringValue = StringValue(input.readUTF())
    override fun serialize(out: DataOutput2, value: StringValue) {
        out.writeUTF(value.value)
    }

    override fun serialize(page: Page, offset: Int, value: StringValue) {
        val localBuffer = this.buffer.get()
        val size = kotlin.math.min(this.logicalSize, value.logicalSize)
        for (i in 0 until size) {
            localBuffer[i] = value.value[i]
        }
        if (size < this.logicalSize) {
            localBuffer[size] = NUL
        }
        page.putChars(offset, localBuffer)
    }

    override fun deserialize(page: Page, offset: Int): StringValue {
        val localBuffer = this.buffer.get()
        val chars = page.getChars(offset, localBuffer)
        val index = localBuffer.indexOfFirst { it == NUL }
        return StringValue(String(chars, 0, index))
    }
}