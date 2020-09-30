package org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates

import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalNodeExpression

/**
 * A [LogicalNodeExpression] that formalizes the execution of a kNN query on a [org.vitrivr.cottontail.model.recordset.Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class KnnLogicalNodeExpression(val predicate: KnnPredicate<*>): UnaryLogicalNodeExpression() {
    /**
     * Returns a copy of this [KnnLogicalNodeExpression]
     *
     * @return Copy of this [KnnLogicalNodeExpression]
     */
    override fun copy(): KnnLogicalNodeExpression = KnnLogicalNodeExpression(this.predicate)
}