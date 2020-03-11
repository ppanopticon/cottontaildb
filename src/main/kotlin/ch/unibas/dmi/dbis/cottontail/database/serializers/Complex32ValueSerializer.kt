package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.Complex32Value
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

object Complex32ValueSerializer : Serializer<Complex32Value> {
    override fun deserialize(input: DataInput2, available: Int): Complex32Value = Complex32Value(floatArrayOf(input.readFloat(), input.readFloat()))
    override fun serialize(out: DataOutput2, value: Complex32Value) {
        out.writeFloat(value.real.value)
        out.writeFloat(value.imaginary.value)
    }
}