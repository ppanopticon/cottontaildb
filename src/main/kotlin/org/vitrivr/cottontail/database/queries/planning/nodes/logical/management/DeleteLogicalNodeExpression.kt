package org.vitrivr.cottontail.database.queries.planning.nodes.logical.management

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalNodeExpression
import org.vitrivr.cottontail.execution.operators.management.DeleteOperator
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [DeleteLogicalNodeExpression] that formalizes a DELETE operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class DeleteLogicalNodeExpression(val entity: Entity): UnaryLogicalNodeExpression() {
    /** The [DeleteLogicalNodeExpression] produces the columns defined in the [DeleteOperator] */
    override val columns: Array<ColumnDef<*>> = DeleteOperator.COLUMNS

    /**
     * Returns a copy of this [DeleteLogicalNodeExpression]
     *
     * @return Copy of this [DeleteLogicalNodeExpression]
     */
    override fun copy(): DeleteLogicalNodeExpression = DeleteLogicalNodeExpression(this.entity)

    /**
     * Calculates and returns the digest for this [DeleteLogicalNodeExpression].
     *
     * @return Digest for this [DeleteLogicalNodeExpression]
     */
    override fun digest(): Long = 31L * super.digest() + this.entity.hashCode()

}