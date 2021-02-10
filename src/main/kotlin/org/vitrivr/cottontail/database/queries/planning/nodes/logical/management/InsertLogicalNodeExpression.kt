package org.vitrivr.cottontail.database.queries.planning.nodes.logical.management

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.binding.RecordBinding
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalNodeExpression
import org.vitrivr.cottontail.execution.operators.management.InsertOperator
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [InsertLogicalNodeExpression] that formalizes a INSERT operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class InsertLogicalNodeExpression(val entity: Entity, val records: MutableList<RecordBinding>) :
    UnaryLogicalNodeExpression() {
    /** The [InsertLogicalNodeExpression] produces the columns defined in the [InsertOperator] */
    override val columns: Array<ColumnDef<*>> = InsertOperator.COLUMNS

    /**
     * Returns a copy of this [DeleteLogicalNodeExpression]
     *
     * @return Copy of this [DeleteLogicalNodeExpression]
     */
    override fun copy(): InsertLogicalNodeExpression =
        InsertLogicalNodeExpression(this.entity, this.records)

    /**
     * Calculates and returns the digest for this [InsertLogicalNodeExpression].
     *
     * @return Digest for this [InsertLogicalNodeExpression]
     */
    override fun digest(): Long {
        var result = 31L * super.digest()
        result = 31 * result + this.entity.hashCode()
        result = 31 * result + this.records.hashCode()
        return result
    }
}