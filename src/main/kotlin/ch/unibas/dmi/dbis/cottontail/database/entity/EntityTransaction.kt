package ch.unibas.dmi.dbis.cottontail.database.entity

import ch.unibas.dmi.dbis.cottontail.database.general.Transaction
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.index.IndexTransaction
import ch.unibas.dmi.dbis.cottontail.database.index.IndexType
import ch.unibas.dmi.dbis.cottontail.model.basics.*
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import kotlin.concurrent.read

/**
 * A [Transaction] that operates on a single [Index]. [Transaction]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 1.1
 */
interface EntityTransaction : Transaction, Filterable, Scanable, Countable, Deletable {
    /**
     * Returns a collection of all the [IndexTransaction] available to this [EntityTransaction].
     *
     * @return Collection of [IndexTransaction]s. May be empty.
     */
    fun indexes(): Collection<IndexTransaction>

    /**
     * Returns a collection of all the [IndexTransaction] available to this [EntityTransaction], that match the given [ColumnDef] and [IndexType] constraint.
     *
     * @param columns The list of [ColumnDef] that should be handled by this [IndexTransaction].
     * @param type The (optional) [IndexType]. If ommitted, [IndexTransaction]s of any type are returned.
     *
     * @return Collection of [IndexTransaction]s. May be empty.
     */
    fun indexes(columns: Array<ColumnDef<*>>? = null, type: IndexType? = null): Collection<IndexTransaction>

    /**
     * Returns the [IndexTransaction] for the given [Name] or null, if such a [IndexTransaction] doesn't exist.
     *
     * @param name The [Name] of the [Index] the [IndexTransaction] belongs to.
     * @return Optional [IndexTransaction]
     */
    fun index(name: Name): IndexTransaction?
}