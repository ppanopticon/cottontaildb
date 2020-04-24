package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.Complex64Value
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2

object Complex64ValueSerializer : Serializer<Complex64Value> {
    override val physicalSize: Int = 2 * Long.SIZE_BYTES
    override val logicalSize: Int = -1

    override fun deserialize(input: DataInput2, available: Int): Complex64Value = Complex64Value(doubleArrayOf(input.readDouble(), input.readDouble()))
    override fun serialize(out: DataOutput2, value: Complex64Value) {
        out.writeDouble(value.real.value)
        out.writeDouble(value.imaginary.value)
    }

    override fun serialize(page: Page, offset: Int, value: Complex64Value) {
        page.putDoubles(offset, value.data)
    }

    override fun deserialize(page: Page, offset: Int): Complex64Value = Complex64Value(page.getDoubles(offset, DoubleArray(2)))
}