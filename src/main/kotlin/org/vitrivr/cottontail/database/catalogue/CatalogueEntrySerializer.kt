package org.vitrivr.cottontail.database.catalogue

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

/**
 * The [Serializer] for a [CatalogueEntry].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal object CatalogueEntrySerializer : Serializer<CatalogueEntry> {
    override fun serialize(out: DataOutput2, value: CatalogueEntry) {
        out.writeUTF(value.name)
        out.writeUTF("")
    }

    override fun deserialize(input: DataInput2, available: Int): CatalogueEntry {
        return CatalogueEntry(input.readUTF())
    }
}