package org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalNodeExpression
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [UnaryLogicalNodeExpression] that represents fetching certain [ColumnDef] from a specific
 * [Entity] and adding them to the list of requested columns.
 *
 * This can be used for late population, which can lead to optimized performance for kNN queries
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class FetchLogicalNodeExpression(val entity: Entity, val fetch: Array<ColumnDef<*>>) : UnaryLogicalNodeExpression() {

    /** The [FetchLogicalNodeExpression] returns the [ColumnDef] of its input + the columns to be fetched. */
    override val columns: Array<ColumnDef<*>>
        get() = (this.input?.columns ?: emptyArray()) + this.fetch

    override fun copy() = FetchLogicalNodeExpression(this.entity, this.fetch)

    /**
     * Calculates and returns the digest for this [FetchLogicalNodeExpression].
     *
     * @return Digest for this [FetchLogicalNodeExpression]
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.entity.hashCode()
        result = 31L * result + this.fetch.contentHashCode()
        return result
    }
}