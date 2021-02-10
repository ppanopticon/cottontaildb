package org.vitrivr.cottontail.database.index

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.general.Tx
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.exceptions.TxException

/**
 * A [Tx] that operates on a single [Index]. [Tx]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
interface IndexTx : Tx, Filterable, Countable {

    /** The simple [Name]s of the [Index] that underpins this [IndexTx] */
    val name: Name

    /** The [ColumnDef]s covered by the [Index] that underpins this [IndexTx]. */
    val columns: Array<ColumnDef<*>>

    /** The [ColumnDef]s produced by the [Index] that underpins this [IndexTx]. */
    val produces: Array<ColumnDef<*>>

    /** The [IndexType] of the [Index] that underpins this [IndexTx]. */
    val type: IndexType

    /**
     * (Re-)builds the underlying [Index] completely.
     *
     * @throws [TxException.TxValidationException] If rebuild of [Index] fails for some reason.
     */
    @Throws(TxException.TxValidationException::class)
    fun rebuild()

    /**
     * Updates the [Index] underlying this [IndexTx] based on the provided [DataChangeEvent].
     *
     * Not all [Index] implementations support incremental updates. Should be indicated by [IndexTransaction#supportsIncrementalUpdate()]
     *
     * @param update [DataChangeEvent] that should be processed.
     * @throws [TxException.TxValidationException] If update of [Index] fails for some reason.
     */
    @Throws(TxException.TxValidationException::class)
    fun update(event: DataChangeEvent)

    /**
     * Performs a lookup through this [IndexTx] and returns a [CloseableIterator] of
     * all the [Record]s that match the [Predicate].
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @return The resulting [CloseableIterator].
     */
    override fun filter(predicate: Predicate): CloseableIterator<Record>

    /**
     * Performs a lookup through this [IndexTx] and returns a [CloseableIterator] of
     * all the [Record]s that match the [Predicate] and fall within the specified data
     * [LongRange], which must lie in 0..[count].
     *
     * Not all [Index] implementations support range filtering.
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @param range The [LongRange] to consider.
     * @return The resulting [CloseableIterator].
     */
    fun filterRange(predicate: Predicate, range: LongRange): CloseableIterator<Record>
}