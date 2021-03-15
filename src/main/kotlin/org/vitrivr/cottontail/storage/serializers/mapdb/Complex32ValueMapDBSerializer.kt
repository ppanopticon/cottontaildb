package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.Complex32Value

/**
 * A [MapDBSerializer] for MapDB based [Complex32Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object Complex32ValueMapDBSerializer : MapDBSerializer<Complex32Value> {
    override fun deserialize(input: DataInput2, available: Int): Complex32Value = Complex32Value(input.readFloat(), input.readFloat())
    override fun serialize(out: DataOutput2, value: Complex32Value) {
        out.writeFloat(value.real.value)
        out.writeFloat(value.imaginary.value)
    }
}