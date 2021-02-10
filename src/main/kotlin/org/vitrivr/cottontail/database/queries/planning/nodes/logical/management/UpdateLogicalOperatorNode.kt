package org.vitrivr.cottontail.database.queries.planning.nodes.logical.management

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.binding.ValueBinding
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.execution.operators.management.UpdateOperator

/**
 * A [DeleteLogicalOperatorNode] that formalizes an UPDATE operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class UpdateLogicalOperatorNode(
    val entity: Entity,
    val values: List<Pair<ColumnDef<*>, ValueBinding>>
) : UnaryLogicalOperatorNode() {
    /** The [UpdateLogicalOperatorNode] does produce the columns defined in the [UpdateOperator]. */
    override val columns: Array<ColumnDef<*>> = UpdateOperator.COLUMNS

    /**
     * Returns a copy of this [UpdateLogicalOperatorNode]
     *
     * @return Copy of this [UpdateLogicalOperatorNode]
     */
    override fun copy(): UpdateLogicalOperatorNode =
        UpdateLogicalOperatorNode(this.entity, this.values)

    /**
     * Calculates and returns the digest for this [UpdateLogicalOperatorNode].
     *
     * @return Digest for this [UpdateLogicalOperatorNode]
     */
    override fun digest(): Long {
        var result = 31L * super.digest()
        result = 31 * result + this.entity.hashCode()
        result = 31 * result + this.values.hashCode()
        return result
    }
}