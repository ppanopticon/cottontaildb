package org.vitrivr.cottontail.database.schema

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.model.exceptions.DatabaseException

/**
 * The header of the [Schema]. Contains information regarding its content!
 *
 * @see Schema
 *
 * @author Ralph Gasser
 * @version 1.0f
 */
class SchemaHeader(val created: Long = System.currentTimeMillis(), var modified: Long = System.currentTimeMillis(), var entities: LongArray = LongArray(0)) {
    companion object {
        /** The identifier that is used to identify a Cottontail DB [Schema] file. */
        internal const val IDENTIFIER: String = "COTTONT_SCM"

        /** The version of the Cottontail DB [Schema]  file. */
        internal const val VERSION: Short = 1
    }
}

/**
 * The [Serializer] for [SchemaHeader]
 */
internal object SchemaHeaderSerializer : Serializer<SchemaHeader> {
    override fun serialize(out: DataOutput2, value: SchemaHeader) {
        out.writeUTF(SchemaHeader.IDENTIFIER)
        out.writeShort(SchemaHeader.VERSION.toInt())
        out.writeLong(value.created)
        out.writeLong(value.modified)
        out.writeInt(value.entities.size)
        for (i in 0 until value.entities.size) {
            out.writeLong(value.entities[i])
        }
    }

    override fun deserialize(input: DataInput2, available: Int): SchemaHeader {
        if (!this.validate(input)) {
            throw DatabaseException.InvalidFileException("Cottontail DB Schema")
        }

        /* Deserialize header. */
        val created = input.readLong()
        val modified = input.readLong()
        val size = input.readInt()
        val entities = LongArray(size)
        for (i in 0 until size) {
            entities[i] = input.readLong()
        }

        /* Return header. */
        return SchemaHeader(created, modified, entities)
    }

    /**
     * Validates the [SchemaHeader]. Must be executed before deserialization
     *
     * @return True if validation was successful, false otherwise.
     */
    private fun validate(input: DataInput2): Boolean {
        val header = input.readUTF()
        val version: Short = input.readShort()
        return (version == SchemaHeader.VERSION) and (header == SchemaHeader.IDENTIFIER)
    }
}