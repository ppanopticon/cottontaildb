package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.Complex32VectorValue

/**
 * A [MapDBSerializer] for MapDB based [Complex32VectorValue] serialization and deserialization.
 *
 * @author Manuel Huerbin
 * @version 1.0.0
 */
class Complex32VectorValueMapDBSerializer(val size: Int) : MapDBSerializer<Complex32VectorValue> {

    init {
        require(this.size > 0) { "Cannot initialize vector value serializer with size value of $size." }
    }

    override fun deserialize(input: DataInput2, available: Int): Complex32VectorValue = Complex32VectorValue(FloatArray(2 * this.size) { input.readFloat() })
    override fun serialize(out: DataOutput2, value: Complex32VectorValue) {
        for (i in 0 until size) {
            out.writeFloat(value.real(i).value)
            out.writeFloat(value.imaginary(i).value)
        }
    }
}