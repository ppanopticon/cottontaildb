package org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates

import org.vitrivr.cottontail.database.queries.binding.KnnPredicateBinding
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalNodeExpression
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.utilities.math.KnnUtilities

/**
 * A [LogicalNodeExpression] that formalizes the execution of a kNN query.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class KnnLogicalNodeExpression(val predicate: KnnPredicateBinding) : UnaryLogicalNodeExpression() {

    /** The [KnnLogicalNodeExpression] returns the [ColumnDef] of its input + a distance column. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
        *(this.input?.columns ?: emptyArray()),
        KnnUtilities.distanceColumnDef(this.predicate.column.name.entity())
    )

    /**
     * Returns a copy of this [KnnLogicalNodeExpression]
     *
     * @return Copy of this [KnnLogicalNodeExpression]
     */
    override fun copy(): KnnLogicalNodeExpression = KnnLogicalNodeExpression(this.predicate)

    /**
     * Calculates and returns the digest for this [KnnLogicalNodeExpression].
     *
     * @return Digest for this [KnnLogicalNodeExpression]
     */
    override fun digest(): Long = 31L * super.digest() + this.predicate.hashCode()
}