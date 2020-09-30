package org.vitrivr.cottontail.model.recordset

import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [Record] implementation as returned and processed by Cottontail DB. A [StandaloneRecord] can exist
 * without an enclosing [Recordset], which is necessary for some applications.
 *
 * <strong>Important:</strong> The use of [StandaloneRecord] is discouraged when data volume becomes large,
 * as each [StandaloneRecord] has its own reference to the [ColumnDef]s it contains.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class StandaloneRecord(override val tupleId: Long = Long.MIN_VALUE, override val columns: Array<ColumnDef<*>>, override val values: Array<Value?>) : Record {

    init {
        /** Sanity check. */
        require(this.values.size == this.columns.size) { "The number of values must be equal to the number of columns held by the StandaloneRecord (v = ${this.values.size}, c = ${this.columns.size})" }
        this.columns.forEachIndexed { index, columnDef ->
            columnDef.validateOrThrow(this.values[index])
        }
    }

    /**
     * Copies this [StandaloneRecord] and returns the copy.
     *
     * @return Copy of this [StandaloneRecord]
     */
    override fun copy(): Record = StandaloneRecord(this.tupleId, this.columns, this.values.copyOf())

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Record

        if (tupleId != other.tupleId) return false
        if (!columns.contentEquals(other.columns)) return false
        if (!values.contentEquals(other.values)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = tupleId.hashCode()
        result = 31 * result + columns.hashCode()
        result = 31 * result + values.contentHashCode()
        return result
    }
}