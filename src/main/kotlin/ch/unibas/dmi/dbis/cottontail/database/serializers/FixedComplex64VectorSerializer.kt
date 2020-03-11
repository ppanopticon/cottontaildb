package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.*
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

/**
 * A [Serializer] for [Complex64VectorValue]s that a fixed in length.
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class FixedComplex64VectorSerializer(val size: Int) : Serializer<Complex64VectorValue> {
    override fun serialize(out: DataOutput2, value: Complex64VectorValue) {
        for (i in 0 until size) {
            out.writeDouble(value.real(i).value)
            out.writeDouble(value.imaginary(i).value)
        }
    }

    override fun deserialize(input: DataInput2, available: Int): Complex64VectorValue {
        val vector = Array(this.size) {
            Complex64Value(DoubleValue(input.readDouble()), DoubleValue(input.readDouble()))
        }
        return Complex64VectorValue(vector)
    }
}