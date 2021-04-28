package org.vitrivr.cottontail.database.index.gg

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.index.IndexConfig
import org.vitrivr.cottontail.math.knn.kernels.Distances

/**
 * Configuration class for [GGIndex].
 *
 * @author Gabriel Zihlmann
 * @version 1.1.0
 */
data class GGIndexConfig(val numGroups: Int, val seed: Long, val distance: Distances) :
    IndexConfig {
    companion object Serializer : org.mapdb.Serializer<GGIndexConfig> {
        const val NUM_SUBSPACES_KEY = "num_groups"
        const val SEED_KEY = "seed"
        const val DISTANCE_KEY = "distance"

        /**
         * Serializes the content of the given value into the given
         * [DataOutput2].
         *
         * @param out DataOutput2 to save object into
         * @param value Object to serialize
         */
        override fun serialize(out: DataOutput2, value: GGIndexConfig) {
            out.packInt(value.numGroups)
            out.packLong(value.seed)
            out.packInt(value.distance.ordinal)
        }

        /**
         * Deserializes and returns the content of the given [DataInput2].
         *
         * @param input DataInput2 to de-serialize data from
         * @param available how many bytes that are available in the DataInput2 for
         * reading, may be -1 (in streams) or 0 (null).
         *
         * @return the de-serialized content of the given [DataInput2]
         */
        override fun deserialize(input: DataInput2, available: Int) = GGIndexConfig(
            numGroups = input.unpackInt(),
            seed = input.unpackLong(),
            distance = Distances.values()[input.unpackInt()]
        )

        /**
         * Constructs a [GGIndexConfig] from a parameter map.
         *
         * @param params The parameter map.
         * @return [GGIndexConfig]
         */
        fun fromParamsMap(params: Map<String, String>) = GGIndexConfig(
            numGroups = params[NUM_SUBSPACES_KEY]?.toIntOrNull() ?: 100,
            seed = params[SEED_KEY]?.toLongOrNull() ?: System.currentTimeMillis(),
            distance = try {
                Distances.valueOf(params[DISTANCE_KEY] ?: "")
            } catch (e: IllegalArgumentException) {
                Distances.L2
            }
        )
    }

    /**
     * Converts this [GGIndexConfig] to a [Map] representation.
     *
     * @return [Map] representation of this [GGIndexConfig].
     */
    override fun toMap(): Map<String, String> = mapOf(
        NUM_SUBSPACES_KEY to this.numGroups.toString(),
        SEED_KEY to this.seed.toString(),
        DISTANCE_KEY to this.distance.toString()
    )
}