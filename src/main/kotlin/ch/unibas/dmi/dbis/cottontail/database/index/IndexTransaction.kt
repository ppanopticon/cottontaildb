package ch.unibas.dmi.dbis.cottontail.database.index

import ch.unibas.dmi.dbis.cottontail.database.general.Transaction
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Filterable
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset

/**
 * A [Transaction] that operates on a single [Index]. [Transaction]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal interface IndexTransaction : Transaction, Filterable {
    /**
     * The [ColumnDef]s covered by the [Index] that underpins this [IndexTransaction].
     */
    val columns: Array<ColumnDef<*>>

    /**
     * The [ColumnDef]s produced by the [Index] that underpins this [IndexTransaction].
     */
    val produces: Array<ColumnDef<*>>

    /**
     * The [IndexType] of the [Index] that underpins this [IndexTransaction].
     */
    val type: IndexType

    /**
     * (Re-)builds the underlying [Index].
     */
    fun rebuild()

    /**
     * Performs a lookup through this [IndexTransaction] and returns a [Recordset].
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @return The resulting [Recordset].
     */
    override fun filter(predicate: Predicate): Recordset

    /**
     * Performs a lookup through this [IndexTransaction] and returns a [Recordset].
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @param parallelism The amount of parallelism to allow for.
     * @return The resulting [Recordset].
     */
    fun lookupParallel(predicate: Predicate, parallelism: Short = 2): Recordset
}