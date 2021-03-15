package org.vitrivr.cottontail.legacy.v1.catalogue

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.schema.DefaultSchema
import org.vitrivr.cottontail.model.exceptions.DatabaseException

/**
 * The header section of the [CatalogueV1] data structure.
 *
 * @see [CatalogueV1]
 *
 * @author Ralph Gasser
 * @version 1.0
 */
data class CatalogueV1Header(
    val size: Long = 0,
    val created: Long = System.currentTimeMillis(),
    val modified: Long = System.currentTimeMillis(),
    val schemas: LongArray = LongArray(0)
) {
    companion object Serializer : org.mapdb.Serializer<CatalogueV1Header> {
        /** The identifier that is used to identify a Cottontail DB [CatalogueV1] file. */
        private const val IDENTIFIER: String = "COTTONT_CAT"

        /** The version of the Cottontail DB [CatalogueV1]  file. */
        private const val VERSION: Short = 1

        override fun serialize(out: DataOutput2, value: CatalogueV1Header) {
            out.writeUTF(IDENTIFIER)
            out.writeShort(VERSION.toInt())
            out.packLong(value.size)
            out.writeLong(value.created)
            out.writeLong(value.modified)
            out.writeInt(value.schemas.size)
            for (i in 0 until value.schemas.size) {
                out.writeLong(value.schemas[i])
            }
        }

        override fun deserialize(input: DataInput2, available: Int): CatalogueV1Header {
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
            return CatalogueV1Header(size, created, modified, schemas)
        }

        /**
         * Validates the [CatalogueV1Header]. Must be executed before deserialization
         *
         * @return True if validation was successful, false otherwise.
         */
        private fun validate(input: DataInput2): Boolean {
            val identifier = input.readUTF()
            val version = input.readShort()
            return (version == VERSION) and (identifier == IDENTIFIER)
        }
    }

    /**
     * An entry in the [CatalogueV1] corresponding to a [DefaultSchema].
     *
     * @see [CatalogueV1]
     * @see [DefaultSchema]
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    data class CatalogueEntry(val name: String) {
        companion object Serializer : org.mapdb.Serializer<CatalogueEntry> {
            override fun serialize(out: DataOutput2, value: CatalogueEntry) {
                out.writeUTF(value.name)
                out.writeUTF("")
            }

            override fun deserialize(input: DataInput2, available: Int): CatalogueEntry {
                return CatalogueEntry(input.readUTF())
            }
        }
    }
}

