package org.vitrivr.cottontail.database.catalogue

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.model.exceptions.DatabaseException

/**
 * The header section of the [Catalogue] data structure.
 *
 * @see [Catalogue]
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal data class CatalogueHeader(val size: Long = 0, val created: Long = System.currentTimeMillis(), val modified: Long = System.currentTimeMillis(), val schemas: LongArray = LongArray(0)) {
    companion object {
        /** The identifier that is used to identify a Cottontail DB [Catalogue] file. */
        internal const val IDENTIFIER: String = "COTTONT_CAT"

        /** The version of the Cottontail DB [Catalogue]  file. */
        internal const val VERSION: Short = 1
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CatalogueHeader

        if (size != other.size) return false
        if (created != other.created) return false
        if (modified != other.modified) return false
        if (!schemas.contentEquals(other.schemas)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = size.hashCode()
        result = 31 * result + created.hashCode()
        result = 31 * result + modified.hashCode()
        result = 31 * result + schemas.contentHashCode()
        return result
    }
}

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

/**
 * An entry in the [Catalogue] corresponding to a [Schema].
 *
 * @see [Catalogue]
 * @see [Schema]
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal data class CatalogueEntry(val name: String)


/**
 * The [Serializer] for a [CatalogueEntry].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal object CatalogueEntrySerializer : Serializer<CatalogueEntry> {
    override fun serialize(out: DataOutput2, value: CatalogueEntry) {
        out.writeUTF(value.name)
        out.writeUTF("") // TODO: Remove; only for backward compatibility
    }

    override fun deserialize(input: DataInput2, available: Int): CatalogueEntry {
        return CatalogueEntry(input.readUTF())
    }
}

