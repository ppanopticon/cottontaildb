package org.vitrivr.cottontail.database.queries.planning.nodes.physical.management

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.RecordBinding
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.management.InsertOperator
import org.vitrivr.cottontail.execution.operators.management.UpdateOperator
import org.vitrivr.cottontail.execution.operators.sources.RecordSourceOperator
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [InsertPhysicalNodeExpression] that formalizes a INSERT operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class InsertPhysicalNodeExpression(val entity: Entity, val records: MutableList<RecordBinding>) :
    UnaryPhysicalNodeExpression() {

    /** The [InsertPhysicalNodeExpression] produces the [ColumnDef]s defined in the [UpdateOperator]. */
    override val columns: Array<ColumnDef<*>> = InsertOperator.COLUMNS

    /** The [InsertPhysicalNodeExpression] produces a single record. */
    override val outputSize: Long = 1L

    /** The [InsertPhysicalNodeExpression]s cost depends on the size of the [records]. */
    override val cost: Cost =
        Cost(io = this.records.size * this.records.first().size * Cost.COST_DISK_ACCESS_WRITE)

    override fun copy(): InsertPhysicalNodeExpression =
        InsertPhysicalNodeExpression(this.entity, this.records)

    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator =
        InsertOperator(RecordSourceOperator(this.records.map { it.apply(ctx) }), this.entity)

    /**
     * Calculates and returns the digest for this [InsertPhysicalNodeExpression].
     *
     * @return Digest for this [InsertPhysicalNodeExpression]e
     */
    override fun digest(): Long {
        var result = 31L * super.digest()
        result = 31 * result + this.entity.hashCode()
        result = 31 * result + this.records.hashCode()
        return result
    }
}