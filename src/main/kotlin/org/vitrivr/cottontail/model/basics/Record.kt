package org.vitrivr.cottontail.model.basics

import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [Record] as returned and processed by Cottontail DB. A [Record] corresponds to a single row and
 * it can hold multiple values, each belonging to a different column (defined by [ColumnDef]s). A [ColumnDef]
 * must not necessarily correspond to a physical database [Column][org.vitrivr.cottontail.database.column.Column].
 *
 * Column-wise access to records through the [Entity][org.vitrivr.cottontail.database.entity.Entity]
 * class returns [Record]s. Furthermore, the interface is
 * used in conjunction with the [Recordset][org.vitrivr.cottontail.model.recordset.Recordset] class.
 *
 * @see org.vitrivr.cottontail.model.recordset.Recordset
 * @see org.vitrivr.cottontail.database.entity.Entity
 *
 * @author Ralph Gasser
 * @version 1.2
 */
interface Record {

    /** The Tuple ID of the [Record]. Usually corresponds to a tupleId in the underlying database. */
    val tupleId: Long

    /** Array of [ColumnDef]s that describes the [Columns][org.vitrivr.cottontail.database.column.Column] of this [Record]. */
    val columns: Array<out ColumnDef<*>>

    /** Array of column values (one entry per column). */
    val values: Array<Value?>

    /** Size of this [Record] in terms of [ColumnDef] it encompasses. */
    val size: Int
        get() = columns.size

    /**
     * Returns the first value in this [Record] or null, if that value is not set.
     *
     * @return First [Value] in this [Record]
     */
    fun first(): Value? = this.values.first()

    /**
     * Returns the last value in this [Record] or null, if that value is not set.
     *
     * @return Last [Value] in this [Record]
     */
    fun last(): Value? = this.values.last()

    /**
     * Creates and returns a copy of this [Record]. The copy is supposed to hold its own copy of the values it holds. However,
     * structural information, such as the columns, may be shared between instances, as they are supposed to be immutable.
     *
     * @return Copy of this [Record].
     */
    fun copy(): Record

    /**
     * Assigns the provided values to this [Record], i.e. the first value is assigned to the first column,
     * the second to the second column etc.
     *
     * @param values The values to assign. Cannot contain more than [Record.size] values.
     *
     * @throws IllegalArgumentException If [values] array size does not match [columns] array size.
     * @throws ValidationException If provided [Value]'s don't match the [ColumnDef].
     */
    fun assign(values: Array<Value?>): Record {
        require(this.values.size == this.columns.size) { "The number of values must be equal to the number of columns held by this record (v = ${this.values.size}, c = ${this.columns.size})" }
        values.forEachIndexed { i, v ->
            this.columns[i].validateOrThrow(v)
            this.values[i] = v
        }
        return this
    }

    /**
     * Returns true, if this [Record] contains the specified [ColumnDef] and false otherwise.
     *
     * @param column The [ColumnDef] specifying the column
     * @return True if record contains the [ColumnDef], false otherwise.
     */
    fun has(column: ColumnDef<*>): Boolean = this.columns.indexOfFirst { it == column } > -1

    /**
     * Generates a Map of the data contained in this [Record]
     *
     * @return Map of column name to value.
     */
    fun toMap(): Map<Name, Value?> = mapOf(*this.columns.mapIndexed { index, column -> Pair(column.name, this.values[index]) }.toTypedArray())

    /**
     * Retrieves the value for the specified [ColumnDef] from this [Record].
     *
     * @param column The [ColumnDef] for which to retrieve the value.
     * @return The value for the [ColumnDef]
     */
    operator fun <T: Value> get(column: ColumnDef<T>): T? {
        val index = this.columns.indexOfFirst { it == column }
        require(index > -1) { "The specified column ${column.name}  (type=${column.type.name})  is not contained in this record." }
        return column.type.cast(this.values[index])
    }

    /**
     * Sets the value for the specified [ColumnDef] in this [Record].
     *
     * @param column The [ColumnDef] for which to set the value.
     * @param value The new value for the [ColumnDef]
     */
    operator fun set(column: ColumnDef<*>, value: Value?) {
        val index = this.columns.indexOfFirst { it == column }
        require(index > -1) { "The specified column ${column.name}  (type=${column.type.name})  is not contained in this record." }
        column.validateOrThrow(value)
        this.values[index] = value
    }
}