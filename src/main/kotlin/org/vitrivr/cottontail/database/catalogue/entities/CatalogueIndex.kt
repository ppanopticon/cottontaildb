package org.vitrivr.cottontail.database.catalogue.entities

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.catalogue.serializers.ColumnNameSerializer
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.model.basics.Name
import java.nio.file.Path
import java.nio.file.Paths

/**
 * The entry that describes a column in Cottontail DB [Catalogue][org.vitrivr.cottontail.database.catalogue.Catalogue].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class CatalogueIndex(val path: Path, val type: IndexType, val columns: List<Name.ColumnName>) {
    /**
     * [Serializer] implementation for [CatalogueIndex].
     */
    object Serializer : org.mapdb.Serializer<CatalogueIndex> {
        override fun serialize(out: DataOutput2, value: CatalogueIndex) {
            out.writeUTF(value.path.toString())
            out.packInt(value.type.ordinal)
            out.packInt(value.columns.size)
            for (c in value.columns) {
                ColumnNameSerializer.serialize(out, c)
            }
        }

        override fun deserialize(input: DataInput2, available: Int): CatalogueIndex = CatalogueIndex(
                Paths.get(input.readUTF()),
                IndexType.values()[input.unpackInt()],
                (0 until input.unpackInt()).map {
                    ColumnNameSerializer.deserialize(input, available)
                }
        )
    }
}