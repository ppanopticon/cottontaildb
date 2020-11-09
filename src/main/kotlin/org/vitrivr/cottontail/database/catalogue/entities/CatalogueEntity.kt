package org.vitrivr.cottontail.database.catalogue.entities

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.catalogue.serializers.ColumnNameSerializer
import org.vitrivr.cottontail.database.catalogue.serializers.IndexNameSerializer
import org.vitrivr.cottontail.model.basics.Name
import java.nio.file.Path
import java.nio.file.Paths

/**
 * The entry in a [Catalogue][org.vitrivr.cottontail.database.catalogue.Catalogue] that describes
 * an [Entity][org.vitrivr.cottontail.database.entity.Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class CatalogueEntity(val path: Path, val columns: List<Name.ColumnName>, val indexes: List<Name.IndexName>) {
    /**
     * [Serializer] implementation for [CatalogueEntity].
     */
    object Serializer : org.mapdb.Serializer<CatalogueEntity> {
        override fun serialize(out: DataOutput2, value: CatalogueEntity) {
            out.writeUTF(value.path.toString())
            out.packInt(value.columns.size)
            for (e in value.columns) {
                ColumnNameSerializer.serialize(out, e)
            }
            out.packInt(value.indexes.size)
            for (e in value.indexes) {
                IndexNameSerializer.serialize(out, e)
            }
        }

        override fun deserialize(input: DataInput2, available: Int): CatalogueEntity = CatalogueEntity(
                Paths.get(input.readUTF()),
                (0 until input.unpackInt()).map {
                    ColumnNameSerializer.deserialize(input, available)
                },
                (0 until input.unpackInt()).map {
                    IndexNameSerializer.deserialize(input, available)
                }
        )
    }
}