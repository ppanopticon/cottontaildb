package org.vitrivr.cottontail.database.index.va.signature

import org.mapdb.DataInput2
import org.mapdb.DataOutput2

/**
 * A fixed length [VAFSignature] used for vector approximation.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.0.0
 */
data class VAFSignature(val tupleId: Long, val cells: IntArray) {
    companion object Serializer : org.mapdb.Serializer<VAFSignature> {
        override fun serialize(out: DataOutput2, value: VAFSignature) {
            out.writeLong(value.tupleId)
            out.packInt(value.cells.size)
            for (c in value.cells) {
                out.writeByte(c)
            }
        }

        override fun deserialize(input: DataInput2, available: Int) = VAFSignature(
            input.readLong(),
            IntArray(input.unpackInt()) { input.readUnsignedByte() }
        )
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as VAFSignature

        if (tupleId != other.tupleId) return false
        if (!cells.contentEquals(other.cells)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = tupleId.hashCode()
        result = 31 * result + cells.contentHashCode()
        return result
    }
}