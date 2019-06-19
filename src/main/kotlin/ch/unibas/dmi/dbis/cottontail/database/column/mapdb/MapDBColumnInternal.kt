package ch.unibas.dmi.dbis.cottontail.database.column.mapdb

import ch.unibas.dmi.dbis.cottontail.model.type.Type
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import ch.unibas.dmi.dbis.cottontail.model.type.TypeFactory
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

/**
 * The header data structure of any [MapDBColumn]
 *
 * @see MapDBColumn
 * @author Ralph Gasser
 * @version 1.0
 */
internal class ColumnHeader(val type: Type<*>, var size: Int = 0, var nullable: Boolean = true, var count: Long = 0, var created: Long = System.currentTimeMillis(), var modified: Long = System.currentTimeMillis()) {
    companion object {
        /** The identifier that is used to identify a Cottontail DB [MapDBColumn] file. */
        internal const val IDENTIFIER: String = "COTTONT_COL"

        /** The version of the Cottontail DB [MapDBColumn] file. */
        internal const val VERSION: Short = 1
    }
}

/**
 * A [Serializer] for [ColumnHeader].
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
internal object ColumnHeaderSerializer: Serializer<ColumnHeader> {
    override fun serialize(out: DataOutput2, value: ColumnHeader) {
        out.writeUTF(ColumnHeader.IDENTIFIER)
        out.writeShort(ColumnHeader.VERSION.toInt())
        out.writeUTF(value.type.name)
        out.writeInt(value.size)
        out.writeBoolean(value.nullable)
        out.packLong(value.count)
        out.writeLong(value.created)
        out.writeLong(value.modified)
    }

    override fun deserialize(input: DataInput2, available: Int): ColumnHeader {
        if (!validate(input)) {
            throw DatabaseException.InvalidFileException("Cottontail DB Column")
        }
        return ColumnHeader(TypeFactory.forName(input.readUTF()), input.readInt(), input.readBoolean(), input.unpackLong(), input.readLong(), input.readLong())
    }

    /**
     * Validates the [ColumnHeader]. Must be executed before deserialization
     *
     * @return True if validation was successful, false otherwise.
     */
    private fun validate(input: DataInput2): Boolean {
        val identifier = input.readUTF()
        val version = input.readShort()
        return (version == ColumnHeader.VERSION) and (identifier == ColumnHeader.IDENTIFIER)
    }
}