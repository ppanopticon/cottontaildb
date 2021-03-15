package org.vitrivr.cottontail.database.entity

import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.schema.DefaultSchema
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.database.statistics.entity.EntityStatistics
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId

/**
 * Represents a single entity in the Cottontail DB data model. An [Entity] has name that must remain
 * unique within a [DefaultSchema]. The [Entity] contains one to many [Column]s holding the actual data.
 * Hence, it can be seen as a table containing tuples.
 *
 * @see Schema
 * @see Column
 * @see EntityTx
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface Entity : DBO {

    /** The [Name.EntityName] of this [Entity]. */
    override val name: Name.EntityName

    /** The [DefaultSchema] this [Entity] belongs to. */
    override val parent: Schema

    /** The [EntityStatistics] in this [Entity]. This is a snapshot and may change immediately. */
    val statistics: EntityStatistics

    /** Number of [Column]s in this [Entity]. */
    val numberOfColumns: Int

    /** Number of entries in this [Entity]. This is a snapshot and may change immediately. */
    val numberOfRows: Long

    /** Estimated maximum [TupleId]s for this [Entity].  This is a snapshot and may change immediately. */
    val maxTupleId: TupleId

    /** Status indicating whether this [Entity] is open or closed and hence can be used or not. */
    override val closed: Boolean

    /**
     * Creates and returns a new [EntityTx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [EntityTx] for.
     * @return New [EntityTx]
     */
    override fun newTx(context: TransactionContext): EntityTx
}