package org.vitrivr.cottontail.database.catalogue.entities

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import java.util.*

/**
 * The header of a [Catalogue][org.vitrivr.cottontail.database.catalogue.Catalogue].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class CatalogueHeader(
        val uuid: UUID = UUID.randomUUID(),
        val version: Int = 1,
        val created: Long = System.currentTimeMillis(),
        val modified: Long = System.currentTimeMillis(),
        val lastOpened: Long = -1L,
        val lastClosed: Long = -1L
) {


    /**
     * [Serializer] implementation for [CatalogueHeader].
     */
    object Serializer : org.mapdb.Serializer<CatalogueHeader> {
        override fun serialize(out: DataOutput2, value: CatalogueHeader) {
            out.writeUTF(value.uuid.toString())
            out.writeInt(value.version)
            out.packLong(value.created)
            out.packLong(value.modified)
            out.packLong(value.lastOpened)
            out.packLong(value.lastClosed)
        }

        override fun deserialize(input: DataInput2, available: Int): CatalogueHeader = CatalogueHeader(
                UUID.fromString(input.readUTF()),
                input.readInt(),
                input.unpackLong(),
                input.unpackLong(),
                input.unpackLong(),
                input.unpackLong()
        )
    }
}