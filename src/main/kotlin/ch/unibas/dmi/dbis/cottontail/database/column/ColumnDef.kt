package ch.unibas.dmi.dbis.cottontail.model.basics

import ch.unibas.dmi.dbis.cottontail.database.column.*
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import ch.unibas.dmi.dbis.cottontail.model.exceptions.ValidationException
import ch.unibas.dmi.dbis.cottontail.model.values.*
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name

import java.lang.RuntimeException

/**
 * A definition class for a Cottontail DB column be it in a DB or in-memory context.  Specifies all the properties of such a and facilitates validation.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class ColumnDef<T: Any> (name: Name, val type: ColumnType<T>, val size: Int = -1, val nullable: Boolean = true) {

    /**
     * Companion object with some convenience methods.
     */
    companion object {
        /**
         * Returns a [ColumnDef] with the provided attributes. The only difference as compared to using the constructor, is that the [ColumnType] can be provided by name.
         *
         * @param name Name of the new [Column]
         * @param type Name of the [ColumnType] of the new [Column]
         * @param size Size of the new [Column] (e.g. for vectors), where eligible.
         * @param nullable Whether or not the [Column] should be nullable.
         */
        fun withAttributes(name: Name, type: String, size: Int = -1, nullable: Boolean = true): ColumnDef<*> = ColumnDef(name.toLowerCase(), ColumnType.forName(type), size, nullable)
    }

    /** The [Name] of this [ColumnDef]. Lower-case values are enforced since Cottontail DB is not case-sensitive! */
    val name = name.toLowerCase()

    /**
     * Validates a value with regard to this [ColumnDef] and throws an Exception, if validation fails.
     *
     * @param value The value that should be validated.
     * @throws [DatabaseException.ValidationException] If validation fails.
     */
    fun validateOrThrow(value: Value<*>?) {
        if (value != null) {
            if (!this.type.compatible(value)) {
                throw ValidationException("The type $type of column '$name' is not compatible with value $value.")
            }
            val cast = this.type.cast(value)
            when {
                cast is DoubleArrayValue && cast.size != this.size -> throw ValidationException("The size of column '$name' (sc=${this.size}) is not compatible with size of value (sv=${cast.size}).")
                cast is FloatArrayValue && cast.size != this.size -> throw ValidationException("The size of column '$name' (sc=${this.size}) is not compatible with size of value (sv=${cast.size}).")
                cast is LongArrayValue && cast.size != this.size -> throw ValidationException("The size of column '$name' (sc=${this.size}) is not compatible with size of value (sv=${cast.size}).")
                cast is IntArrayValue && cast.size != this.size -> throw ValidationException("The size of column '$name' (sc=${this.size}) is not compatible with size of value (sv=${cast.size}).")
            }
        } else if (!this.nullable) {
            throw ValidationException("The column '$name' cannot be null!")
        }
    }

    /**
     * Validates a value with regard to this [ColumnDef] return a flag indicating whether validation was passed.
     *
     * @param value The value that should be validated.
     * @return True if value passes validation, false otherwise.
     */
    fun validate(value: Value<*>?) : Boolean {
        if (value != null) {
            if (!this.type.compatible(value)) {
                return false
            }
            val cast = this.type.cast(value)
            return when {
                cast is DoubleArrayValue && cast.size != this.size -> false
                cast is FloatArrayValue && cast.size != this.size -> false
                cast is LongArrayValue && cast.size != this.size -> false
                cast is IntArrayValue && cast.size != this.size -> false
                else -> true
            }
        } else return this.nullable
    }

    /**
     * Returns the default value for this [ColumnDef].
     *
     * @return Default value for this [ColumnDef].
     */
    fun defaultValue(): Value<*>? = when {
        this.nullable -> null
        this.type is StringColumnType -> StringValue("")
        this.type is FloatColumnType -> FloatValue(0.0f)
        this.type is DoubleColumnType -> DoubleValue(0.0)
        this.type is IntColumnType -> IntValue(0)
        this.type is LongColumnType -> LongValue(0L)
        this.type is ShortColumnType -> ShortValue(0.toShort())
        this.type is ByteColumnType -> ByteValue(0.toByte())
        this.type is BooleanColumnType -> BooleanValue(false)
        this.type is DoubleArrayColumnType -> DoubleArrayValue(DoubleArray(this.size))
        this.type is FloatArrayColumnType -> FloatArrayValue(FloatArray(this.size))
        this.type is LongArrayColumnType -> LongArrayValue(LongArray(this.size))
        this.type is IntArrayColumnType -> IntArrayValue(IntArray(this.size))
        else -> throw RuntimeException("Default value for the specified type $type has not been specified yet!")
    }


    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ColumnDef<*>

        if (name != other.name) return false
        if (type != other.type) return false
        if (size != other.size) return false

        return true
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + type.hashCode()
        result = 31 * result + size.hashCode()
        return result
    }

    override fun toString(): String = "$name(type=$type, size=$size, nullable=$nullable)"
}