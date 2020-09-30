package org.vitrivr.cottontail.database.index.lsh.superbit

import org.mapdb.DataInput2
import org.mapdb.DataOutput2

data class SuperBitLSHIndexConfig(val buckets: Int, val stages: Int, val seed: Long) {
    companion object Serializer: org.mapdb.Serializer<SuperBitLSHIndexConfig> {
        override fun serialize(out: DataOutput2, value: SuperBitLSHIndexConfig) {
            out.packInt(value.buckets)
            out.packInt(value.stages)
            out.packLong(value.seed)
        }

        override fun deserialize(input: DataInput2, available: Int): SuperBitLSHIndexConfig = SuperBitLSHIndexConfig(input.unpackInt(), input.unpackInt(), input.unpackLong())
    }
}

