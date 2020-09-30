package org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates

import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalNodeExpression

/**
 * A [LogicalNodeExpression] that formalizes filtering using some [BooleanPredicate].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FilterLogicalNodeExpression(val predicate: BooleanPredicate): UnaryLogicalNodeExpression() {
    /**
     * Returns a copy of this [KnnLogicalNodeExpression]
     *
     * @return Copy of this [KnnLogicalNodeExpression]
     */
    override fun copy(): FilterLogicalNodeExpression = FilterLogicalNodeExpression(this.predicate)
}