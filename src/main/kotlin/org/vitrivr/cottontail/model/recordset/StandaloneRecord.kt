package org.vitrivr.cottontail.model.recordset

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [Record] implementation as returned and processed by Cottontail DB. A [StandaloneRecord] can
 * exist without an enclosing [Recordset], which is necessary for some applications.
 *
 * <strong>Important:</strong> The use of [StandaloneRecord] is discouraged when data volume becomes
 * large, as each [StandaloneRecord] has its own reference to the [ColumnDef]s it contains.
 *
 * @author Ralph Gasser
 * @version 1.0.2
 */
class StandaloneRecord(override var tupleId: Long = Long.MIN_VALUE, override val columns: Array<ColumnDef<*>>, private val values: Array<Value?> = Array(columns.size) { null }) : Record {

    init {
        /** Sanity check. */
        require(values.size == this.columns.size) { "The number of values must be equal to the number of columns held by the StandaloneRecord (v = ${values.size}, c = ${this.columns.size})" }
        this.columns.forEachIndexed { index, columnDef ->
            if (!columnDef.validate(values[index])) {
                throw IllegalArgumentException("Provided value ${values[index]} is incompatible with column ${columnDef}.")
            }
        }
    }

    /** Initialize internal [Object2ObjectOpenHashMap] used to map columns to values. */
    //private val map = Object2ObjectOpenHashMap(this.columns, values)

    /**
     * Copies this [StandaloneRecord] and returns the copy.
     *
     * @return Copy of this [StandaloneRecord]
     */
    override fun copy(): Record = StandaloneRecord(this.tupleId, this.columns, this.values)

    /**
     * Iterates over the [ColumnDef] and [Value] pairs in this [Record] in the order specified by [columns].
     *
     * @param action The action to apply to each [ColumnDef], [Value] pair.
     */
    override fun forEach(action: (ColumnDef<*>, Value?) -> Unit) = this.columns.forEachIndexed { index, columnDef ->
        action(columnDef, this.values[index])
    }

    /**
     * Returns true, if this [StandaloneRecord] contains the specified [ColumnDef] and false otherwise.
     *
     * @param column The [ColumnDef] specifying the column
     * @return True if record contains the [ColumnDef], false otherwise.
     */
    override fun has(column: ColumnDef<*>): Boolean = this.columns.contains(column)

    /**
     * Returns an unmodifiable [Map] of the data contained in this [StandaloneRecord].
     *
     * @return Unmodifiable [Map] of the data in this [StandaloneRecord].
     */
    override fun toMap(): Map<ColumnDef<*>, Value?> = this.columns.mapIndexed { index, columnDef -> columnDef to this.values[index] }.toMap()

    /**
     * Retrieves the value for the specified [ColumnDef] from this [StandaloneRecord].
     *
     * @param column The [ColumnDef] for which to retrieve the value.
     * @return The value for the [ColumnDef]
     */
    override fun get(column: ColumnDef<*>): Value? {
        val index = this.columns.indexOf(column)
        require(index > -1) { "The specified column ${column.name}  (type=${column.type.name}) is not contained in this record." }
        return this.values[index]
    }

    /**
     * Sets the value for the specified [ColumnDef] in this [StandaloneRecord].
     *
     * @param column The [ColumnDef] for which to set the value.
     * @param value The new value for the [ColumnDef]
     */
    override fun set(column: ColumnDef<*>, value: Value?) {
        val index = this.columns.indexOf(column)
        require(index > -1) { "The specified column ${column.name} (type=${column.type.name})  is not contained in this record." }
        if (!column.validate(value)) {
            throw IllegalArgumentException("Provided value $value is incompatible with column $column.")
        }
        this.values[index] = value
    }


    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as StandaloneRecord

        if (tupleId != other.tupleId) return false
        if (!this.columns.contentDeepEquals(other.columns)) return false
        if (!this.values.contentDeepEquals(other.values)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = tupleId.hashCode()
        result = 31 * result + columns.hashCode()
        result = 31 * result + values.hashCode()
        return result
    }
}