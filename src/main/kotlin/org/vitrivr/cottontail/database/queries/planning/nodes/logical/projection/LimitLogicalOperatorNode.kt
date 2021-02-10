package org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode

/**
 * A [UnaryLogicalOperatorNode] that represents the application of a LIMIT and/or SKIP clause on the
 * final result [org.vitrivr.cottontail.model.recordset.Recordset].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class LimitLogicalOperatorNode(limit: Long, skip: Long) : UnaryLogicalOperatorNode() {

    /** The [LimitLogicalOperatorNode] returns the [ColumnDef] of its input, or no column at all. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input?.columns ?: emptyArray()

    /** Number of records to limit the result set to. */
    val limit = if (limit.coerceAtLeast(0) == 0L) {
        Long.MAX_VALUE
    } else {
        limit
    }

    /** Number of records to skip before limiting the result set. */
    val skip = if (limit.coerceAtLeast(0) == 0L) {
        0L
    } else {
        skip
    }

    /**
     * Returns a copy of this [LimitLogicalOperatorNode]
     *
     * @return Copy of this [LimitLogicalOperatorNode]
     */
    override fun copy(): LimitLogicalOperatorNode = LimitLogicalOperatorNode(this.limit, this.skip)

    /**
     * Calculates and returns the digest for this [LimitLogicalOperatorNode].
     *
     * @return Digest for this [LimitLogicalOperatorNode]
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.limit.hashCode()
        result = 31L * result + this.skip.hashCode()
        return result
    }
}