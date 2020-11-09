package org.vitrivr.cottontail.database.catalogue.entities

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.catalogue.serializers.EntityNameSerializer
import org.vitrivr.cottontail.model.basics.Name
import java.nio.file.Path
import java.nio.file.Paths

/**
 * The entry that describes a schema in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class CatalogueSchema(val path: Path, val entities: List<Name.EntityName>) {
    /**
     * [Serializer] implementation for [CatalogueSchema].
     */
    object Serializer : org.mapdb.Serializer<CatalogueSchema> {
        override fun serialize(out: DataOutput2, value: CatalogueSchema) {
            out.writeUTF(value.path.toString())
            out.packInt(value.entities.size)
            for (e in value.entities) {
                EntityNameSerializer.serialize(out, e)
            }
        }

        override fun deserialize(input: DataInput2, available: Int): CatalogueSchema = CatalogueSchema(
                Paths.get(input.readUTF()),
                (0 until input.unpackInt()).map {
                    EntityNameSerializer.deserialize(input, available)
                }
        )
    }
}