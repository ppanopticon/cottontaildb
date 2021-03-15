package org.vitrivr.cottontail.legacy.v1.schema

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.exceptions.DatabaseException

/**
 * The header of the [SchemaV1]. Contains information regarding its content!
 *
 * @see SchemaV1
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class SchemaV1Header(
    val created: Long = System.currentTimeMillis(),
    var modified: Long = System.currentTimeMillis(),
    var entities: LongArray = LongArray(0)
) {
    companion object Serializer : org.mapdb.Serializer<SchemaV1Header> {
        /** The identifier that is used to identify a Cottontail DB [SchemaV1] file. */
        private const val IDENTIFIER: String = "COTTONT_SCM"

        /** The version of the Cottontail DB [SchemaV1]  file. */
        private const val VERSION: Short = 1

        override fun serialize(out: DataOutput2, value: SchemaV1Header) {
            out.writeUTF(IDENTIFIER)
            out.writeShort(VERSION.toInt())
            out.writeLong(value.created)
            out.writeLong(value.modified)
            out.writeInt(value.entities.size)
            for (element in value.entities) {
                out.writeLong(element)
            }
        }

        override fun deserialize(input: DataInput2, available: Int): SchemaV1Header {
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
            return SchemaV1Header(created, modified, entities)
        }

        /**
         * Validates the [SchemaV1Header]. Must be executed before deserialization
         *
         * @return True if validation was successful, false otherwise.
         */
        private fun validate(input: DataInput2): Boolean {
            val header = input.readUTF()
            val version: Short = input.readShort()
            return (version == VERSION) and (header == IDENTIFIER)
        }
    }
}