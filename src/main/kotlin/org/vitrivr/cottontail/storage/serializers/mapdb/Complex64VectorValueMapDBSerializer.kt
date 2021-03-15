package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.Complex64VectorValue

/**
 * A [MapDBSerializer] for MapDB based [Complex64VectorValue] serialization and deserialization.
 *
 * @author Manuel Huerbin
 * @version 1.0.0
 */
class Complex64VectorValueMapDBSerializer(val size: Int) : MapDBSerializer<Complex64VectorValue> {

    init {
        require(this.size > 0) { "Cannot initialize vector value serializer with size value of $size." }
    }

    override fun deserialize(input: DataInput2, available: Int): Complex64VectorValue = Complex64VectorValue(DoubleArray(2 * this.size) { input.readDouble() })
    override fun serialize(out: DataOutput2, value: Complex64VectorValue) {
        for (i in 0 until size) {
            out.writeDouble(value.real(i).value)
            out.writeDouble(value.imaginary(i).value)
        }
    }
}