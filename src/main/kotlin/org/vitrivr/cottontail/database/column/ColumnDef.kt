package org.vitrivr.cottontail.database.column

import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.types.Value


/**
 * A definition class for a Cottontail DB column be it in a DB or in-memory context
 * Specifies all the properties of such a and facilitates validation.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
data class ColumnDef<T : Value>(
    val name: Name.ColumnName,
    val type: Type<T>,
    val nullable: Boolean = true,
    val primary: Boolean = false
) {

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

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ColumnDef<*>

        if (name != other.name) return false
        if (type != other.type) return false

        return true
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + type.hashCode()
        return result
    }

    override fun toString(): String = "$name(type=$type, nullable=$nullable)"
}