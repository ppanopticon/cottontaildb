package org.vitrivr.cottontail.database.entity

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.column.ColumnEngine
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.model.exceptions.DatabaseException

/**
 * The header section of the [DefaultEntity] data structure.
 *
 * @see DefaultEntity
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
data class EntityHeader(
    var name: String,
    var created: Long = System.currentTimeMillis(),
    var modified: Long = System.currentTimeMillis(),
    var columns: List<ColumnRef> = emptyList(),
    var indexes: List<IndexRef> = emptyList()
) {
    companion object Serializer : org.mapdb.Serializer<EntityHeader> {
        override fun serialize(out: DataOutput2, value: EntityHeader) {
            out.packInt(DBOVersion.V2_0.ordinal)
            out.writeUTF(value.name)
            out.writeLong(value.created)
            out.writeLong(value.modified)
            out.packInt(value.columns.size)
            value.columns.forEach { ColumnRef.serialize(out, it) }
            out.packInt(value.indexes.size)
            value.indexes.forEach { IndexRef.serialize(out, it) }
        }

        override fun deserialize(input: DataInput2, available: Int): EntityHeader {
            val version = DBOVersion.values()[input.unpackInt()]
            if (version != DBOVersion.V2_0)
                throw DatabaseException.VersionMismatchException(version, DBOVersion.V2_0)
            return EntityHeader(
                input.readUTF(),
                input.readLong(),
                input.readLong(),
                (0 until input.unpackInt()).map { ColumnRef.deserialize(input, available) },
                (0 until input.unpackInt()).map { IndexRef.deserialize(input, available) }
            )
        }
    }

    /**
     * Reference pointing to a column.
     */
    data class ColumnRef(val name: String, val type: ColumnEngine) {
        companion object Serializer : org.mapdb.Serializer<ColumnRef> {
            override fun serialize(out: DataOutput2, value: ColumnRef) {
                out.writeUTF(value.name)
                out.packInt(value.type.ordinal)
            }

            override fun deserialize(input: DataInput2, available: Int): ColumnRef = ColumnRef(
                input.readUTF(),
                ColumnEngine.values()[input.unpackInt()]
            )
        }
    }

    /**
     * Reference pointing to an index.
     */
    data class IndexRef(val name: String, val type: IndexType) {
        companion object Serializer : org.mapdb.Serializer<IndexRef> {
            override fun serialize(out: DataOutput2, value: IndexRef) {
                out.writeUTF(value.name)
                out.packInt(value.type.ordinal)
            }

            override fun deserialize(input: DataInput2, available: Int): IndexRef = IndexRef(
                input.readUTF(),
                IndexType.values()[input.unpackInt()]
            )
        }
    }
}