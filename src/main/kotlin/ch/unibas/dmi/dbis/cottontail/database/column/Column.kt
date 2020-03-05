package ch.unibas.dmi.dbis.cottontail.database.column

import ch.unibas.dmi.dbis.cottontail.database.column.mapdb.MapDBColumn
import ch.unibas.dmi.dbis.cottontail.database.general.DBO
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value

import java.util.*

/**
 *
 */
interface Column<T: Value> : DBO {
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
        get() = this.columnDef.size

    /**
     * Whether or not this [Column] is nullable. Columns that are not nullable, cannot hold any
     * null values.
     *
     * @return Nullability property of this [Column].
     */
    val nullable: Boolean
        get() = this.columnDef.nullable

    /**
     * The maximum tuple ID used by this [Column].
     */
    val maxTupleId: Long

    /**
     * Creates a new [ColumnTransaction] and returns it.
     *
     * @param readonly True, if the resulting [MapDBColumn.Tx] should be a read-only transaction.
     * @param tid The ID for the new [MapDBColumn.Tx]
     *
     * @return A new [ColumnTransaction] object.
     */
    fun newTransaction(readonly: Boolean = false, tid: UUID = UUID.randomUUID()) : ColumnTransaction<T>
}