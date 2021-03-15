package org.vitrivr.cottontail.legacy.v1.column

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.DatabaseException

/**
 * The header data structure of any [ColumnV1]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ColumnV1Header(
    val type: Type<*>,
    var size: Int = 0,
    var nullable: Boolean = true,
    var count: Long = 0,
    var created: Long = System.currentTimeMillis(),
    var modified: Long = System.currentTimeMillis()
) {
    companion object Serializer : org.mapdb.Serializer<ColumnV1Header> {
        /** The identifier that is used to identify a Cottontail DB [MapDBColumn] file. */
        private const val IDENTIFIER: String = "COTTONT_COL"

        /** The version of the Cottontail DB [MapDBColumn] file. */
        private const val VERSION: Short = 1

        override fun serialize(out: DataOutput2, value: ColumnV1Header) {
            out.writeUTF(IDENTIFIER)
            out.writeShort(VERSION.toInt())
            out.writeUTF(value.type.name)
            out.writeInt(value.size)
            out.writeBoolean(value.nullable)
            out.packLong(value.count)
            out.writeLong(value.created)
            out.writeLong(value.modified)
        }

        override fun deserialize(input: DataInput2, available: Int): ColumnV1Header {
            if (!validate(input)) {
                throw DatabaseException.InvalidFileException("Cottontail DB Column")
            }
            val type = Type.forName(input.readUTF(), input.readInt())
            return ColumnV1Header(
                type,
                type.logicalSize,
                input.readBoolean(),
                input.unpackLong(),
                input.readLong(),
                input.readLong()
            )
        }

        /**
         * Validates the [ColumnHeader]. Must be executed before deserialization
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