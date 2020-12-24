package org.vitrivr.cottontail.database.column

import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [DBO] in the Cottontail DB data model that represents a [Column]. A [Column] can hold values
 * of a given type, as specified by the [ColumnDef].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface Column<T: Value> : DBO {
    /** The [Name.ColumnName] of this [Column]. */
    override val name: Name.ColumnName

    /**
     * This [Column]'s [ColumnDef]. It contains all the relevant information that defines a [Column]
     *
     * @return [ColumnDef] for this [Column]
     */
    val columnDef: ColumnDef<T>

    /**
     * This [Column]'s type.
     *
     * @return The [ColumnType] of this [Column].
     */
    val type: ColumnType<T>
        get() = this.columnDef.type

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
     * Creates a new [ColumnTx] for the given [TransactionContext].
     *
     * @param context [TransactionContext] to create [ColumnTx] for.
     */
    override fun newTx(context: TransactionContext): ColumnTx<T>
}