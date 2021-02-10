package org.vitrivr.cottontail.database.queries.planning.nodes.logical.management

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.execution.operators.management.DeleteOperator

/**
 * A [DeleteLogicalOperatorNode] that formalizes a DELETE operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class DeleteLogicalOperatorNode(val entity: Entity) : UnaryLogicalOperatorNode() {
    /** The [DeleteLogicalOperatorNode] produces the columns defined in the [DeleteOperator] */
    override val columns: Array<ColumnDef<*>> = DeleteOperator.COLUMNS

    /**
     * Returns a copy of this [DeleteLogicalOperatorNode]
     *
     * @return Copy of this [DeleteLogicalOperatorNode]
     */
    override fun copy(): DeleteLogicalOperatorNode = DeleteLogicalOperatorNode(this.entity)

    /**
     * Calculates and returns the digest for this [DeleteLogicalOperatorNode].
     *
     * @return Digest for this [DeleteLogicalOperatorNode]
     */
    override fun digest(): Long = 31L * super.digest() + this.entity.hashCode()

}