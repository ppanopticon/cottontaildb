package org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.utilities.math.KnnUtilities

/**
 * A [LogicalOperatorNode] that formalizes the execution of a kNN query.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class KnnLogicalOperatorNode(val predicate: KnnPredicate) : UnaryLogicalOperatorNode() {

    /** The [KnnLogicalOperatorNode] returns the [ColumnDef] of its input + a distance column. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
        *(this.input?.columns ?: emptyArray()),
        KnnUtilities.distanceColumnDef(this.predicate.column.name.entity())
    )

    /**
     * Returns a copy of this [KnnLogicalOperatorNode]
     *
     * @return Copy of this [KnnLogicalOperatorNode]
     */
    override fun copy(): KnnLogicalOperatorNode = KnnLogicalOperatorNode(this.predicate)

    /**
     * Calculates and returns the digest for this [KnnLogicalOperatorNode].
     *
     * @return Digest for this [KnnLogicalOperatorNode]
     */
    override fun digest(): Long = 31L * super.digest() + this.predicate.digest()
}