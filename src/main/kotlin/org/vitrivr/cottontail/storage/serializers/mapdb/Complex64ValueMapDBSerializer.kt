package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.Complex64Value

/**
 * A [MapDBSerializer] for MapDB based [Complex64Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object Complex64ValueMapDBSerializer : MapDBSerializer<Complex64Value> {
    override fun deserialize(input: DataInput2, available: Int): Complex64Value = Complex64Value(doubleArrayOf(input.readDouble(), input.readDouble()))
    override fun serialize(out: DataOutput2, value: Complex64Value) {
        out.writeDouble(value.real.value)
        out.writeDouble(value.imaginary.value)
    }
}