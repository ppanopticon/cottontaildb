package org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalNodeExpression

/**
 * A [UnaryLogicalNodeExpression] that represents the application of a LIMIT and/or SKIP clause on the
 * final result [org.vitrivr.cottontail.model.recordset.Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class LimitLogicalNodeExpression(limit: Long, skip: Long): UnaryLogicalNodeExpression() {

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
     * Returns a copy of this [LimitLogicalNodeExpression]
     *
     * @return Copy of this [LimitLogicalNodeExpression]
     */
    override fun copy(): LimitLogicalNodeExpression = LimitLogicalNodeExpression(this.limit, this.skip)
}