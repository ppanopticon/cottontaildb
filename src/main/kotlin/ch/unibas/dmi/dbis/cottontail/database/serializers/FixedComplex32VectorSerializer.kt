package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.Complex32Value
import ch.unibas.dmi.dbis.cottontail.model.values.Complex32VectorValue
import ch.unibas.dmi.dbis.cottontail.model.values.FloatValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel

/**
 * A [Serializer] for [Complex32VectorValue]s that a fixed in length.
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class FixedComplex32VectorSerializer(override val logicalSize: Int) : Serializer<Complex32VectorValue> {
    override fun serialize(out: DataOutput2, value: Complex32VectorValue) {
        for (i in 0 until this.logicalSize) {
            out.writeFloat(value.real(i).value)
            out.writeFloat(value.imaginary(i).value)
        }
    }

    override fun deserialize(input: DataInput2, available: Int): Complex32VectorValue {
        val vector = Array(this.logicalSize) {
            Complex32Value(FloatValue(input.readFloat()), FloatValue(input.readFloat()))
        }
        return Complex32VectorValue(vector)
    }

    override val physicalSize: Int = this.logicalSize * 2 * Int.Companion.SIZE_BYTES
    override fun serialize(channel: WritableByteChannel, value: Complex32VectorValue) {
        TODO("Not yet implemented")
    }

    override fun deserialize(channel: ReadableByteChannel): Complex32VectorValue {
        TODO("Not yet implemented")
    }
}