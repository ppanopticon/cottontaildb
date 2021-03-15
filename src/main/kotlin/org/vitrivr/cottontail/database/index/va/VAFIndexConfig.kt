package org.vitrivr.cottontail.database.index.va

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.index.IndexConfig

/**
 * A configuration class for the [VAFIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class VAFIndexConfig(val marksPerDimension: Int) : IndexConfig {
    companion object Serializer : org.mapdb.Serializer<VAFIndexConfig> {
        const val MARKS_PER_DIMENSION_KEY = "marks_per_dimension"

        override fun serialize(out: DataOutput2, value: VAFIndexConfig) {
            out.packInt(value.marksPerDimension)
        }

        override fun deserialize(input: DataInput2, available: Int) =
            VAFIndexConfig(input.unpackInt())

        /**
         * Constructs a [VAFIndexConfig] from a parameter map.
         *
         * @param params The parameter map.
         * @return VAFIndexConfig
         */
        fun fromParamMap(params: Map<String, String>) =
            VAFIndexConfig(params[MARKS_PER_DIMENSION_KEY]?.toIntOrNull() ?: 10)
    }

    /**
     * Converts this [VAFIndexConfig] to a [Map] representation.
     *
     * @return [Map] representation of this [VAFIndexConfig].
     */
    override fun toMap(): Map<String, String> = mapOf(
        MARKS_PER_DIMENSION_KEY to this.marksPerDimension.toString()
    )
}
