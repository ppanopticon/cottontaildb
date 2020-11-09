package org.vitrivr.cottontail.database.catalogue.entities

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.basics.TupleId

/**
 * An entry in a [Catalogue][org.vitrivr.cottontail.database.catalogue.Catalogue] that collects
 * statistics about an [Entity][org.vitrivr.cottontail.database.entity.Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class CatalogueEntityStatistics(val size: Long, val maxTupleId: TupleId) {
    /**
     * [Serializer] implementation for [CatalogueEntityStatistics].
     */
    object Serializer : org.mapdb.Serializer<CatalogueEntityStatistics> {
        override fun serialize(out: DataOutput2, value: CatalogueEntityStatistics) {
            out.packLong(value.size)
            out.packLong(value.maxTupleId)
        }

        override fun deserialize(input: DataInput2, available: Int): CatalogueEntityStatistics = CatalogueEntityStatistics(
                input.unpackLong(),
                input.unpackLong()
        )
    }
}