package org.vitrivr.cottontail.database.column

import org.vitrivr.cottontail.database.column.mapdb.MapDBColumn
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.values.types.Value

import java.util.*

/**
 * A [Column] in Cottontail DB. [Column] are the data structures that hold the actual data  found in the database.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
interface Column<T: Value> : DBO {
    /**
     * This [Column]'s [ColumnDef]. It contains all the relevant information that defines a [Column]
     *
     * @return [ColumnDef] for this [Column]
     */
    val columnDef: ColumnDef<T>

    /**
     * Size of the content of this [Column]. The size is -1 (undefined) for most type of [Column]s.
     * However, some column types like those holding arrays may have a defined size property
     *
     * @return size of this [Column].
     */
    val size: Int
        get() = this.columnDef.logicalSize

    /**
     * Whether or not this [Column] is nullable. Columns that are not nullable, cannot hold any
     * null values.
     *
     * @return Nullability property of this [Column].
     */
    val nullable: Boolean
        get() = this.columnDef.nullable

    /**
     * Creates a new [ColumnTransaction] and returns it.
     *
     * @param readonly True, if the resulting [MapDBColumn.Tx] should be a read-only transaction.
     * @param tid The ID for the new [MapDBColumn.Tx]
     *
     * @return A new [ColumnTransaction] object.
     */
    fun newTransaction(readonly: Boolean = false, tid: UUID = UUID.randomUUID()): ColumnTransaction<T>
}