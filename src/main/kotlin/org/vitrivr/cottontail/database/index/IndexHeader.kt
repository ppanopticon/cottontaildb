package org.vitrivr.cottontail.database.index

import org.mapdb.DataInput2
import org.mapdb.DataOutput2

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.model.exceptions.DatabaseException

/**
 * The header section of an [AbstractIndex] data structure.
 *
 * @see AbstractIndex
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
data class IndexHeader(
    val name: String,
    val type: IndexType,
    val columns: Array<ColumnDef<*>>,
    val created: Long = System.currentTimeMillis(), val modified: Long = System.currentTimeMillis()
) {

    companion object Serializer : org.mapdb.Serializer<IndexHeader> {
        override fun serialize(out: DataOutput2, value: IndexHeader) {
            out.packInt(DBOVersion.V2_0.ordinal)
            out.writeUTF(value.name)
            out.packInt(value.type.ordinal)
            out.packInt(value.columns.size)
            value.columns.forEach { ColumnDef.serialize(out, it) }
        }

        override fun deserialize(input: DataInput2, available: Int): IndexHeader {
            val version = DBOVersion.values()[input.unpackInt()]
            if (version != DBOVersion.V2_0)
                throw DatabaseException.VersionMismatchException(version, DBOVersion.V2_0)
            return IndexHeader(
                input.readUTF(),
                IndexType.values()[input.unpackInt()],
                Array(input.unpackInt()) { ColumnDef.deserialize(input, available) }
            )
        }
    }
}