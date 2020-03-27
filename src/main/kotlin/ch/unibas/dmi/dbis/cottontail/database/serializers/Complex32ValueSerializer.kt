package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.Complex32Value
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2

object Complex32ValueSerializer : Serializer<Complex32Value> {
    override fun deserialize(input: DataInput2, available: Int): Complex32Value = Complex32Value(floatArrayOf(input.readFloat(), input.readFloat()))
    override fun serialize(out: DataOutput2, value: Complex32Value) {
        out.writeFloat(value.real.value)
        out.writeFloat(value.imaginary.value)
    }
    override val physicalSize: Int = 2 * Int.SIZE_BYTES
    override val logicalSize: Int = -1
    override fun serialize(page: Page, offset: Int, value: Complex32Value) {
        TODO("Not yet implemented")
    }

    override fun deserialize(page: Page, offset: Int): Complex32Value {
        TODO("Not yet implemented")
    }

}