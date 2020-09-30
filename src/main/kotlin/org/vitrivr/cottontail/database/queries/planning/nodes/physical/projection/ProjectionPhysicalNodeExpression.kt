package org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection

import org.vitrivr.cottontail.database.queries.components.Projection
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.projection.*
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * Formalizes a [UnaryPhysicalNodeExpression] operation in the Cottontail DB query execution engine.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
data class ProjectionPhysicalNodeExpression(val type: Projection, val fields: List<Pair<Name.ColumnName, Name.ColumnName?>>) : UnaryPhysicalNodeExpression() {
    init {
        /* Sanity check. */
        when (type) {
            Projection.SELECT,
            Projection.SELECT_DISTINCT,
            Projection.COUNT_DISTINCT -> if (this.fields.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify at least one column.")
            }
            Projection.MEAN,
            Projection.SUM,
            Projection.MIN,
            Projection.MAX -> if (fields.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify a column.")
            }
            else -> {
            }
        }
    }

    override val outputSize: Long
        get() = this.input.outputSize

    override val cost: Cost
        get() = Cost(cpu = this.outputSize * this.fields.size * Cost.COST_MEMORY_ACCESS_READ)

    override fun copy() = ProjectionPhysicalNodeExpression(this.type, this.fields)
    override fun toOperator(context: ExecutionEngine.ExecutionContext) = when (this.type) {
        Projection.SELECT -> SelectProjectionOperator(this.input.toOperator(context), context, this.fields)
        Projection.SELECT_DISTINCT -> TODO()
        Projection.COUNT -> CountProjectionOperator(this.input.toOperator(context), context)
        Projection.COUNT_DISTINCT -> TODO()
        Projection.EXISTS -> ExistsProjectionOperator(this.input.toOperator(context), context)
        Projection.SUM -> SumProjectionOperator(this.input.toOperator(context), context, this.fields.first().first)
        Projection.MAX -> MaxProjectionOperator(this.input.toOperator(context), context, this.fields.first().first)
        Projection.MIN -> MinProjectionOperator(this.input.toOperator(context), context, this.fields.first().first)
        Projection.MEAN -> MeanProjectionOperator(this.input.toOperator(context), context, this.fields.first().first)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ProjectionPhysicalNodeExpression

        if (type != other.type) return false
        if (fields != other.fields) return false

        return true
    }

    override fun hashCode(): Int {
        var result = type.hashCode()
        result = 31 * result + fields.hashCode()
        return result
    }
}