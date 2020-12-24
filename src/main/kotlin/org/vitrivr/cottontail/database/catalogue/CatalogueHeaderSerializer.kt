package org.vitrivr.cottontail.database.catalogue

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.model.exceptions.DatabaseException

/**
 * The [Serializer] for the [CatalogueHeader].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal object CatalogueHeaderSerializer : Serializer<CatalogueHeader> {
    override fun serialize(out: DataOutput2, value: CatalogueHeader) {
        out.writeUTF(CatalogueHeader.IDENTIFIER)
        out.writeShort(CatalogueHeader.VERSION.toInt())
        out.packLong(value.size)
        out.writeLong(value.created)
        out.writeLong(value.modified)
        out.writeInt(value.schemas.size)
        for (i in 0 until value.schemas.size) {
            out.writeLong(value.schemas[i])
        }
    }

    override fun deserialize(input: DataInput2, available: Int): CatalogueHeader {
        if (!validate(input)) {
            throw DatabaseException.InvalidFileException("Cottontail DB Entity")
        }
        val size = input.unpackLong()
        val created = input.readLong()
        val modified = input.readLong()
        val schema_count = input.readInt()
        val schemas = LongArray(schema_count)
        for (i in 0 until schema_count) {
            schemas[i] = input.readLong()
        }
        return CatalogueHeader(size, created, modified, schemas)
    }

    /**
     * Validates the [CatalogueHeader]. Must be executed before deserialization
     *
     * @return True if validation was successful, false otherwise.
     */
    private fun validate(input: DataInput2): Boolean {
        val identifier = input.readUTF()
        val version = input.readShort()
        return (version == CatalogueHeader.VERSION) and (identifier == CatalogueHeader.IDENTIFIER)
    }
}