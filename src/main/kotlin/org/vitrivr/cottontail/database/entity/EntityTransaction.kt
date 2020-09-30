package org.vitrivr.cottontail.database.entity

import org.vitrivr.cottontail.database.general.Transaction
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTransaction
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.model.basics.*

/**
 * A [Transaction] that operates on a single [Index]. [Transaction]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 1.2
 */
interface EntityTransaction : Transaction, Scanable, Countable, Modifiable {

    /** Reference to the [Entity] affected by this [EntityTransaction]. */
    val entity: Entity

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
     * Returns the [IndexTransaction] for the given [Name.IndexName] or null, if such a [IndexTransaction] doesn't exist.
     *
     * @param name The [Name.IndexName] of the [Index] the [IndexTransaction] belongs to.
     * @return Optional [IndexTransaction]
     */
    fun index(name: Name.IndexName): IndexTransaction?
}