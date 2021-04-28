package org.vitrivr.cottontail.database.index

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.general.Tx
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.model.basics.Countable
import org.vitrivr.cottontail.model.basics.Filterable
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.TxException

/**
 * A [Tx] that operates on a single [AbstractIndex]. [Tx]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 1.8.1
 */
interface IndexTx : Tx, Filterable, Countable {

    /** Reference to the [Index] this [IndexTx] belongs to. */
    override val dbo: Index

    /** The order in which results of this [IndexTx] appear. Empty array that there is no particular order. */
    val order: Array<Pair<ColumnDef<*>, SortOrder>>

    /** The [IndexType] of the [AbstractIndex] that underpins this [IndexTx]. */
    val type: IndexType

    /**
     * (Re-)builds the underlying [AbstractIndex] completely.
     *
     * @throws [TxException.TxValidationException] If rebuild of [AbstractIndex] fails for some reason.
     */
    @Throws(TxException.TxValidationException::class)
    fun rebuild()

    /**
     * Updates the [AbstractIndex] underlying this [IndexTx] based on the provided [DataChangeEvent].
     *
     * Not all [AbstractIndex] implementations support incremental updates. Should be indicated by [IndexTransaction#supportsIncrementalUpdate()]
     *
     * @param event [DataChangeEvent] that should be processed.
     * @throws [TxException.TxValidationException] If update of [AbstractIndex] fails for some reason.
     */
    @Throws(TxException.TxValidationException::class)
    fun update(event: DataChangeEvent)

    /**
     * Clears the [AbstractIndex] underlying this [IndexTx] and removes all entries it contains.
     *
     * @throws [TxException.TxValidationException] If update of [AbstractIndex] fails for some reason.
     */
    @Throws(TxException.TxValidationException::class)
    fun clear()

    /**
     * Performs a lookup through this [IndexTx] and returns a [Iterator] of
     * all the [Record]s that match the [Predicate].
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @return The resulting [Iterator].
     */
    override fun filter(predicate: Predicate): Iterator<Record>

    /**
     * Performs a lookup through this [IndexTx] and returns a [Iterator] of
     * all the [Record]s that match the [Predicate] and fall within the specified data
     * [LongRange], which must lie in 0..[count].
     *
     * Not all [AbstractIndex] implementations support range filtering.
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @param partitionIndex The [partitionIndex] for this [filterRange] call.
     * @param partitions The total number of partitions for this [filterRange] call.
     * @return The resulting [Iterator].
     */
    fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int): Iterator<Record>
}