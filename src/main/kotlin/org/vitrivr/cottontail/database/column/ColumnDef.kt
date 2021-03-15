package org.vitrivr.cottontail.database.column

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.types.Value


/**
 * A definition class for a Cottontail DB column be it in a DB or in-memory context
 * Specifies all the properties of such a and facilitates validation.
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
data class ColumnDef<T : Value>(val name: Name.ColumnName, val type: Type<T>, val nullable: Boolean = true, val primary: Boolean = false) {

    /**
     * [org.mapdb.Serializer] for [ColumnDef].
     */
    companion object Serializer : org.mapdb.Serializer<ColumnDef<*>> {
        override fun serialize(out: DataOutput2, value: ColumnDef<*>) {
            out.writeUTF(value.name.toString())
            out.packInt(value.type.ordinal)
            out.packInt(value.type.logicalSize)
            out.writeBoolean(value.nullable)
            out.writeBoolean(value.primary)
        }

        override fun deserialize(input: DataInput2, available: Int): ColumnDef<*> = ColumnDef(
            Name.ColumnName(*input.readUTF().split('.').toTypedArray()),
            Type.forOrdinal(input.unpackInt(), input.unpackInt()),
            input.readBoolean(),
            input.readBoolean()
        )
    }

    /**
     * Validates a value with regard to this [ColumnDef] return a flag indicating whether validation was passed.
     *
     * @param value The value that should be validated.
     * @return True if value passes validation, false otherwise.
     */
    fun validate(value: Value?): Boolean {
        return if (value != null) {
            this.type.compatible(value)
        } else this.nullable
    }

    override fun toString(): String = "$name(type=$type, nullable=$nullable)"
}