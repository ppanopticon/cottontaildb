package org.vitrivr.cottontail.database.index

import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.general.Transaction
import org.vitrivr.cottontail.database.queries.components.Predicate
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.exceptions.ValidationException

/**
 * A [Transaction] that operates on a single [Index]. [Transaction]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 1.4
 */
interface IndexTransaction : Transaction, Filterable {
    /** The simple [Name]s of the [Index] that underpins this [IndexTransaction] */
    val name: Name

    /** The [ColumnDef]s covered by the [Index] that underpins this [IndexTransaction]. */
    val columns: Array<ColumnDef<*>>

    /** The [ColumnDef]s produced by the [Index] that underpins this [IndexTransaction]. */
    val produces: Array<ColumnDef<*>>

    /** The [IndexType] of the [Index] that underpins this [IndexTransaction]. */
    val type: IndexType

    /** True, if the [Index] underpinning this [IndexTransaction] supports incremental updates, and false otherwise. */
    val supportsIncrementalUpdate: Boolean

    /**
     * (Re-)builds the underlying [Index] completely.
     *
     * @throws [ValidationException.IndexUpdateException] If rebuild of [Index] fails for some reason.
     */
    @Throws(ValidationException.IndexUpdateException::class)
    fun rebuild()

    /**
     * Updates the [Index] underlying this [IndexTransaction] based on the provided [DataChangeEvent].
     *
     * Not all [Index] implementations support incremental updates. Should be indicated by [IndexTransaction#supportsIncrementalUpdate()]
     *
     * @param update Collection of [Record]s to updated wrapped by the corresponding [DataChangeEvent].
     * @throws [ValidationException.IndexUpdateException] If rebuild of [Index] fails for some reason.
     */
    @Throws(ValidationException.IndexUpdateException::class)
    fun update(update: Collection<DataChangeEvent>)

    /**
     * Performs a lookup through this [IndexTransaction] and returns a [CloseableIterator] of
     * all the [Record]s that match the [Predicate].
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @return The resulting [CloseableIterator].
     */
    override fun filter(predicate: Predicate): CloseableIterator<Record>
}