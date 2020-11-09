package org.vitrivr.cottontail.database.entity

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.database.column.ColumnDriver
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.exceptions.DatabaseException

/**
 * The header section of the [Entity] data structure.
 *
 * @see Entity
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class EntityHeader(var size: Long = 0, var maxTupleId: TupleId = -1L, var created: Long = System.currentTimeMillis(), var modified: Long = System.currentTimeMillis(), var columns: LongArray = LongArray(0), var indexes: LongArray = LongArray(0)) {
    companion object {
        /** The identifier that is used to identify a Cottontail DB [Entity] file. */
        internal const val IDENTIFIER: String = "COTTONT_ENT"

        /** The version of the Cottontail DB [Entity]  file. */
        internal const val VERSION: Short = 1
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

/**
 * The [Serializer] for the [EntityHeader].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
internal object EntityHeaderSerializer : Serializer<EntityHeader> {
    override fun serialize(out: DataOutput2, value: EntityHeader) {
        out.writeUTF(EntityHeader.IDENTIFIER)
        out.writeShort(EntityHeader.VERSION.toInt())
        out.packLong(value.size)
        out.packLong(value.maxTupleId)
        out.packLong(value.created)
        out.packLong(value.modified)
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
        val maxTupleId = input.unpackLong()
        val created = input.unpackLong()
        val modified = input.unpackLong()
        val columns = LongArray(input.readShort().toInt())
        for (i in columns.indices) {
            columns[i] = input.unpackLong()
        }
        val indexes = LongArray(input.readShort().toInt())
        for (i in indexes.indices) {
            indexes[i] = input.unpackLong()
        }
        return EntityHeader(size, maxTupleId, created, modified, columns, indexes)
    }

    /**
     * Validates the [EntityHeader]. Must be executed before deserialization
     *
     * @return True if validation was successful, false otherwise.
     */
    private fun validate(input: DataInput2): Boolean {
        val identifier = input.readUTF()
        val version = input.readShort()
        return (version == EntityHeader.VERSION) and (identifier == EntityHeader.IDENTIFIER)
    }
}

/**
 * An entry pointing to a [Column][org.vitrivr.cottontail.database.column.Column]
 *
 * @see Entity
 * @see org.vitrivr.cottontail.database.column.Column
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
internal data class ColumnEntry(val name: String, val driver: ColumnDriver)

/**
 * The [Serializer] for the [ColumnEntry].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal object ColumnEntrySerializer : Serializer<ColumnEntry> {
    override fun serialize(out: DataOutput2, value: ColumnEntry) {
        out.writeUTF(value.name)
        out.writeInt(value.driver.ordinal)
    }

    override fun deserialize(input: DataInput2, available: Int): ColumnEntry = try {
        ColumnEntry(input.readUTF(), ColumnDriver.values()[input.readInt()])
    } catch (e: IllegalArgumentException) {
        throw DatabaseException.DataCorruptionException("Unsupported index type: ${e.message}")
    }
}

/**
 * An entry pointing to an [Index][org.vitrivr.cottontail.database.index.Index]
 *
 * @see Entity
 * @see org.vitrivr.cottontail.database.index.Index
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
internal data class IndexEntry(val name: String, val type: IndexType, val dirty: Boolean, val columns: Array<String>) {
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

/**
 * The [Serializer] for the [IndexEntry].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal object IndexEntrySerializer : Serializer<IndexEntry> {
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