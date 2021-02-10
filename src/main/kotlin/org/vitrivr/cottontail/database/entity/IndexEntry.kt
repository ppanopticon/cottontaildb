package org.vitrivr.cottontail.database.entity

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.model.exceptions.DatabaseException

/**
 * An entry pointing to an [Index][org.vitrivr.cottontail.database.index.Index]
 *
 * @see Entity
 * @see org.vitrivr.cottontail.database.index.Index
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class IndexEntry(
    val name: String,
    val type: IndexType,
    val dirty: Boolean,
    val columns: Array<String>
) {

    /**
     * The [Serializer] for the [IndexEntry].
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    companion object Serializer : org.mapdb.Serializer<IndexEntry> {
        override fun serialize(out: DataOutput2, value: IndexEntry) {
            out.writeUTF(value.name)
            out.writeUTF(value.type.name)
            out.writeBoolean(value.dirty)
            out.writeInt(value.columns.size)
            value.columns.forEach { out.writeUTF(it) }
        }

        override fun deserialize(input: DataInput2, available: Int): IndexEntry = try {
            val name = input.readUTF()
            val type = IndexType.valueOf(input.readUTF())
            val dirty = input.readBoolean()
            val length = input.readInt()
            val columns = Array<String>(length) { input.readUTF() }
            IndexEntry(name, type, dirty, columns)
        } catch (e: IllegalArgumentException) {
            throw DatabaseException.DataCorruptionException("Unsupported index type: ${e.message}")
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as IndexEntry

        if (name != other.name) return false
        if (type != other.type) return false
        if (dirty != other.dirty) return false
        if (!columns.contentEquals(other.columns)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + type.hashCode()
        result = 31 * result + dirty.hashCode()
        result = 31 * result + columns.contentHashCode()
        return result
    }
}