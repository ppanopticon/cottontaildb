package org.vitrivr.cottontail.database.index.pq

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.index.IndexConfig

/**
 * Configuration class for [PQIndex].
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.1.0
 */
data class PQIndexConfig(
    val numSubspaces: Int,
    val numCentroids: Int,
    val sampleSize: Int,
    val seed: Long
) : IndexConfig {

    companion object Serializer : org.mapdb.Serializer<PQIndexConfig> {

        const val AUTO_VALUE = -1
        const val NUM_SUBSPACES_KEY = "num_subspaces"
        const val NUM_CENTROIDS_KEY = "num_centroids"
        const val SAMPLE_SIZE = "sample_size"
        const val SEED_KEY = "seed"

        /**
         * Serializes the content of the given value into the given
         * [DataOutput2].
         *
         * @param out DataOutput2 to save object into
         * @param value Object to serialize
         *
         * @throws IOException in case of an I/O error
         */
        override fun serialize(out: DataOutput2, value: PQIndexConfig) {
            out.packInt(value.numSubspaces)
            out.packInt(value.numCentroids)
            out.packInt(value.sampleSize)
            out.packLong(value.seed)
        }

        /**
         * Deserializes and returns the content of the given [DataInput2].
         *
         * @param input DataInput2 to de-serialize data from
         * @param available how many bytes that are available in the DataInput2 for
         * reading, may be -1 (in streams) or 0 (null).
         *
         * @return the de-serialized content of the given [DataInput2]
         * @throws IOException in case of an I/O error
         */
        override fun deserialize(input: DataInput2, available: Int) = PQIndexConfig(
            input.unpackInt(),
            input.unpackInt(),
            input.unpackInt(),
            input.unpackLong()
        )

        /**
         * Constructs a [PQIndexConfig] from a parameter map.
         *
         * @param params The parameter map.
         * @return [PQIndexConfig]
         */
        fun fromParamMap(params: Map<String, String>) = PQIndexConfig(
            params[NUM_SUBSPACES_KEY]?.toInt() ?: AUTO_VALUE,
            params[NUM_CENTROIDS_KEY]?.toInt() ?: 100,
            params[SAMPLE_SIZE]?.toInt() ?: 1500,
            params[SEED_KEY]?.toLongOrNull() ?: System.currentTimeMillis()
        )
    }

    /**
     * Converts this [PQIndexConfig] to a [Map] representation.
     *
     * @return [Map] representation of this [PQIndexConfig].
     */
    override fun toMap(): Map<String, String> = mapOf(
        NUM_SUBSPACES_KEY to this.numSubspaces.toString(),
        NUM_CENTROIDS_KEY to this.numCentroids.toString(),
        SAMPLE_SIZE to this.sampleSize.toString(),
        SEED_KEY to this.seed.toString()
    )
}