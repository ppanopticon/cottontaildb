package org.vitrivr.cottontail.database.index.pq

import org.mapdb.DataInput2
import org.mapdb.DataOutput2

/**
 * An inverted file entry in the [PQIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class PQIndexEntry(val signature: PQSignature, val tupleIds: LongArray) {

    companion object Serializer : org.mapdb.Serializer<PQIndexEntry> {
        override fun serialize(out: DataOutput2, value: PQIndexEntry) {
            PQSignature.serialize(out, value.signature)
            org.mapdb.Serializer.LONG_ARRAY.serialize(out, value.tupleIds)
        }

        override fun deserialize(input: DataInput2, available: Int): PQIndexEntry = PQIndexEntry(
            PQSignature.deserialize(input, available),
            org.mapdb.Serializer.LONG_ARRAY.deserialize(input, available)
        )
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as PQIndexEntry

        if (signature != other.signature) return false
        if (!tupleIds.contentEquals(other.tupleIds)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = signature.hashCode()
        result = 31 * result + tupleIds.contentHashCode()
        return result
    }
}