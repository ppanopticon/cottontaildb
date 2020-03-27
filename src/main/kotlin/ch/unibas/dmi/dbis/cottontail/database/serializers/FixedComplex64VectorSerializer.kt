package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.Complex64Value
import ch.unibas.dmi.dbis.cottontail.model.values.Complex64VectorValue
import ch.unibas.dmi.dbis.cottontail.model.values.DoubleValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2

/**
 * A [Serializer] for [Complex64VectorValue]s that a fixed in length.
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class FixedComplex64VectorSerializer(override val logicalSize: Int) : Serializer<Complex64VectorValue> {
    override fun serialize(out: DataOutput2, value: Complex64VectorValue) {
        for (i in 0 until this.logicalSize) {
            out.writeDouble(value.real(i).value)
            out.writeDouble(value.imaginary(i).value)
        }
    }

    override fun deserialize(input: DataInput2, available: Int): Complex64VectorValue {
        val vector = Array(this.logicalSize) {
            Complex64Value(DoubleValue(input.readDouble()), DoubleValue(input.readDouble()))
        }
        return Complex64VectorValue(vector)
    }

    override val physicalSize: Int = this.logicalSize * 2 * Long.Companion.SIZE_BYTES
    override fun serialize(page: Page, offset: Int, value: Complex64VectorValue) {
        TODO("Not yet implemented")
    }

    override fun deserialize(page: Page, offset: Int): Complex64VectorValue {
        TODO("Not yet implemented")
    }

}