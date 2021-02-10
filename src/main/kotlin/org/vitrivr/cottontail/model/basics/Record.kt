package org.vitrivr.cottontail.model.basics

import org.vitrivr.cottontail.database.column.ColumnDef
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
 * @version 1.3.0
 */
interface Record {

    /** The [TupleId] of this [Record]. Can be updated! */
    var tupleId: TupleId

    /** [Array] of [ColumnDef]s contained in this [Record]. */
    val columns: Array<ColumnDef<*>>

    /** Size of this [Record] in terms of [ColumnDef] it encompasses. */
    val size: Int
        get() = this.columns.size

    /**
     * Creates and returns a copy of this [Record]. The copy is supposed to hold its own copy of the values it holds. However,
     * structural information, such as the columns, may be shared between instances, as they are supposed to be immutable.
     *
     * @return Copy of this [Record].
     */
    fun copy(): Record

    /**
     * Iterates over the [ColumnDef] and [Value] pairs in this [Record] in the order specified by [columns].
     *
     * @param action The action to apply to each [ColumnDef], [Value] pair.
     */
    fun forEach(action: (ColumnDef<*>, Value?) -> Unit)

    /**
     * Returns true, if this [Record] contains the specified [ColumnDef] and false otherwise.
     *
     * @param column The [ColumnDef] specifying the column
     * @return True if record contains the [ColumnDef], false otherwise.
     */
    fun has(column: ColumnDef<*>): Boolean

    /**
     * Generates and returns a [Map] of the data contained in this [Record]
     *
     * @return Map of column name to value.
     */
    fun toMap(): Map<ColumnDef<*>, Value?>

    /**
     * Retrieves the value for the specified [ColumnDef] from this [Record].
     *
     * @param column The [ColumnDef] for which to retrieve the value.
     * @return The value for the [ColumnDef]
     */
    operator fun get(column: ColumnDef<*>): Value?

    /**
     * Sets the value for the specified [ColumnDef] in this [MutableRecord].
     *
     * @param column The [ColumnDef] for which to set the value.
     * @param value The new value for the [ColumnDef]
     */
    operator fun set(column: ColumnDef<*>, value: Value?)
}