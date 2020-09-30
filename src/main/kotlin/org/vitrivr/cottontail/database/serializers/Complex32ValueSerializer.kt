package org.vitrivr.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.Complex32Value
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer

object Complex32ValueSerializer : Serializer<Complex32Value> {
    override val physicalSize: Int = 2 * Int.SIZE_BYTES
    override val logicalSize: Int = -1

    override fun deserialize(input: DataInput2, available: Int): Complex32Value = Complex32Value(floatArrayOf(input.readFloat(), input.readFloat()))
    override fun serialize(out: DataOutput2, value: Complex32Value) {
        out.writeFloat(value.real.value)
        out.writeFloat(value.imaginary.value)
    }

    override fun serialize(page: Page, offset: Int, value: Complex32Value) {
        page.putFloats(offset, value.data)
    }

    override fun deserialize(page: Page, offset: Int): Complex32Value = Complex32Value(page.getFloats(offset, FloatArray(2)))
}