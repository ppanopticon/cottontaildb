package ch.unibas.dmi.dbis.cottontail.database.index

import ch.unibas.dmi.dbis.cottontail.database.events.DataChangeEvent
import ch.unibas.dmi.dbis.cottontail.database.general.Transaction
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate

import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Filterable
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.exceptions.ValidationException
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name

/**
 * A [Transaction] that operates on a single [Index]. [Transaction]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 1.2
 */
interface IndexTransaction : Transaction, Filterable {
    /** The simple [Name]s of the [Index] that underpins this [IndexTransaction] */
    val name: Name

    /** The fqn [Name]s of the [Index] that underpins this [IndexTransaction] */
    val fqn: Name

    /** The [ColumnDef]s covered by the [Index] that underpins this [IndexTransaction]. */
    val columns: Array<ColumnDef<*>>

    /** The [ColumnDef]s produced by the [Index] that underpins this [IndexTransaction]. */
    val produces: Array<ColumnDef<*>>

    /** The [IndexType] of the [Index] that underpins this [IndexTransaction]. */
    val type: IndexType

    /**
     * Returns true, if the [Index] underpinning this [IndexTransaction] supports incremental updates, and false otherwise.
     *
     * @return True if incremental [Index] updates are supported.
     */
    fun supportsIncrementalUpdate(): Boolean

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
     * Performs a lookup through this [IndexTransaction] and returns a [Recordset].
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @return The resulting [Recordset].
     */
    override fun filter(predicate: Predicate): Recordset
}