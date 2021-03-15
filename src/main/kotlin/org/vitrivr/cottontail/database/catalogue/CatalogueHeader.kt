package org.vitrivr.cottontail.database.catalogue

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*

/**
 * The header section of the [DefaultCatalogue] data structure.
 *
 * @see [DefaultCatalogue]
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
internal data class CatalogueHeader(
    val uid: String = UUID.randomUUID().toString(),
    val created: Long = System.currentTimeMillis(),
    val modified: Long = System.currentTimeMillis(),
    val schemas: List<SchemaRef> = emptyList()
) {

    companion object Serializer : org.mapdb.Serializer<CatalogueHeader> {
        override fun serialize(out: DataOutput2, value: CatalogueHeader) {
            out.packInt(DBOVersion.V2_0.ordinal)
            out.writeUTF(value.uid)
            out.writeLong(value.created)
            out.writeLong(value.modified)
            out.packInt(value.schemas.size)
            value.schemas.forEach { SchemaRef.serialize(out, it) }
        }

        override fun deserialize(input: DataInput2, available: Int): CatalogueHeader {
            val version = DBOVersion.values()[input.unpackInt()]
            if (version != DBOVersion.V2_0)
                throw DatabaseException.VersionMismatchException(version, DBOVersion.V2_0)
            return CatalogueHeader(
                input.readUTF(),
                input.readLong(),
                input.readLong(),
                (0 until input.unpackInt()).map { SchemaRef.deserialize(input, available) }
            )
        }
    }

    /**
     * A reference to a schema.
     */
    data class SchemaRef(val name: String, val path: Path? = null) {
        companion object Serializer : org.mapdb.Serializer<SchemaRef> {
            override fun serialize(out: DataOutput2, value: SchemaRef) {
                out.writeUTF(value.name)
                out.writeBoolean(value.path != null)
                if (value.path != null) {
                    out.writeUTF(value.path.toString())
                }
            }

            override fun deserialize(input: DataInput2, available: Int): SchemaRef {
                return SchemaRef(
                    input.readUTF(), if (input.readBoolean()) {
                        Paths.get(input.readUTF())
                    } else {
                        null
                    }
                )
            }
        }
    }
}
