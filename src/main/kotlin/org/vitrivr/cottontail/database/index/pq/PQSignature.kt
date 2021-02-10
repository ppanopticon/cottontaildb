package org.vitrivr.cottontail.database.index.pq

import org.mapdb.DataInput2
import org.mapdb.DataOutput2

/**
 * A [PQSignature] as used by the [PQIndex]. Wraps an [IntArray].
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.0.0
 */
class PQSignature(val cells: IntArray) {
    companion object Serializer : org.mapdb.Serializer<PQSignature> {
        override fun serialize(out: DataOutput2, value: PQSignature) {
            out.packInt(value.cells.size)
            for (i in value.cells) {
                out.packInt(i)
            }
        }

        override fun deserialize(input: DataInput2, available: Int): PQSignature {
            val array = IntArray(input.unpackInt()) {
                input.unpackInt()
            }
            return PQSignature(array)
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as PQSignature

        if (!this.cells.contentEquals(other.cells)) return false

        return true
    }

    override fun hashCode(): Int = this.cells.contentHashCode()
}