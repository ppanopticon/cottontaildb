package org.vitrivr.cottontail.database.entity

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.exceptions.DatabaseException

/**
 * The header section of the [Entity] data structure.
 *
 * @see Entity
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
internal data class EntityHeader(
    var size: Long = 0,
    var created: Long = System.currentTimeMillis(),
    var modified: Long = System.currentTimeMillis(),
    var columns: LongArray = LongArray(0),
    var indexes: LongArray = LongArray(0)
) {
    companion object Serializer : org.mapdb.Serializer<EntityHeader> {
        /** The identifier that is used to identify a Cottontail DB [Entity] file. */
        const val IDENTIFIER: String = "COTTONT_ENT"

        /** The version of the Cottontail DB [Entity]  file. */
        const val VERSION: Short = 1

        override fun serialize(out: DataOutput2, value: EntityHeader) {
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

        override fun deserialize(input: DataInput2, available: Int): EntityHeader {
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
            return EntityHeader(size, created, modified, columns, indexes)
        }

        /**
         * Validates the [EntityHeader]. Must be executed before deserialization
         *
         * @return True if validation was successful, false otherwise.
         */
        private fun validate(input: DataInput2): Boolean {
            val identifier = input.readUTF()
            val version = input.readShort()
            return (version == VERSION) and (identifier == IDENTIFIER)
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as EntityHeader

        if (size != other.size) return false
        if (created != other.created) return false
        if (modified != other.modified) return false
        if (!columns.contentEquals(other.columns)) return false
        if (!indexes.contentEquals(other.indexes)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = size.hashCode()
        result = 31 * result + created.hashCode()
        result = 31 * result + modified.hashCode()
        result = 31 * result + columns.contentHashCode()
        result = 31 * result + indexes.contentHashCode()
        return result
    }
}