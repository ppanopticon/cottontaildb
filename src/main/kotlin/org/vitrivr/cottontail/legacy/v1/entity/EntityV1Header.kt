package org.vitrivr.cottontail.legacy.v1.entity

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.exceptions.DatabaseException

/**
 * The header section of the [EntityV1] data structure.
 *
 * @see EntityV1
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
data class EntityV1Header(
    var size: Long = 0,
    var created: Long = System.currentTimeMillis(),
    var modified: Long = System.currentTimeMillis(),
    var columns: LongArray = LongArray(0),
    var indexes: LongArray = LongArray(0)
) {
    companion object Serializer : org.mapdb.Serializer<EntityV1Header> {
        /** The identifier that is used to identify a Cottontail DB [EntityV1] file. */
        private const val IDENTIFIER: String = "COTTONT_ENT"

        /** The version of the Cottontail DB [EntityV1]  file. */
        private const val VERSION: Short = 1

        override fun serialize(out: DataOutput2, value: EntityV1Header) {
            out.writeUTF(IDENTIFIER)
            out.writeShort(VERSION.toInt())
            out.packLong(value.size)
            out.writeLong(value.created)
            out.writeLong(value.modified)
            out.writeShort(value.columns.size)
            value.columns.forEach { out.packLong(it) }
            out.writeShort(value.indexes.size)
            value.indexes.forEach { out.packLong(it) }
        }

        override fun deserialize(input: DataInput2, available: Int): EntityV1Header {
            if (!validate(input)) {
                throw DatabaseException.InvalidFileException("Cottontail DB Entity")
            }
            val size = input.unpackLong()
            val created = input.readLong()
            val modified = input.readLong()
            val columns = LongArray(input.readShort().toInt())
            for (i in columns.indices) {
                columns[i] = input.unpackLong()
            }
            val indexes = LongArray(input.readShort().toInt())
            for (i in indexes.indices) {
                indexes[i] = input.unpackLong()
            }
            return EntityV1Header(size, created, modified, columns, indexes)
        }

        /**
         * Validates the [EntityV1Header]. Must be executed before deserialization
         *
         * @return True if validation was successful, false otherwise.
         */
        private fun validate(input: DataInput2): Boolean {
            val identifier = input.readUTF()
            val version = input.readShort()
            return (version == VERSION) and (identifier == IDENTIFIER)
        }
    }
}