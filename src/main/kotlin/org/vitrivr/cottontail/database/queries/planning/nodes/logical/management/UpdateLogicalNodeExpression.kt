package org.vitrivr.cottontail.database.queries.planning.nodes.logical.management

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.binding.ValueBinding
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalNodeExpression
import org.vitrivr.cottontail.execution.operators.management.UpdateOperator
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [DeleteLogicalNodeExpression] that formalizes an UPDATE operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class UpdateLogicalNodeExpression(
    val entity: Entity,
    val values: List<Pair<ColumnDef<*>, ValueBinding>>
) : UnaryLogicalNodeExpression() {
    /** The [UpdateLogicalNodeExpression] does produce the columns defined in the [UpdateOperator]. */
    override val columns: Array<ColumnDef<*>> = UpdateOperator.COLUMNS

    /**
     * Returns a copy of this [UpdateLogicalNodeExpression]
     *
     * @return Copy of this [UpdateLogicalNodeExpression]
     */
    override fun copy(): UpdateLogicalNodeExpression =
        UpdateLogicalNodeExpression(this.entity, this.values)

    /**
     * Calculates and returns the digest for this [UpdateLogicalNodeExpression].
     *
     * @return Digest for this [UpdateLogicalNodeExpression]
     */
    override fun digest(): Long {
        var result = 31L * super.digest()
        result = 31 * result + this.entity.hashCode()
        result = 31 * result + this.values.hashCode()
        return result
    }
}