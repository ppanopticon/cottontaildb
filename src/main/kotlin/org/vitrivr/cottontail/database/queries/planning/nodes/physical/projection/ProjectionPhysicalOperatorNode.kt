package org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.ProjectionLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.projection.*
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * Formalizes a [UnaryPhysicalOperatorNode] operation in the Cottontail DB query execution engine.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class ProjectionPhysicalOperatorNode(
    val type: Projection,
    val fields: List<Pair<ColumnDef<*>, Name.ColumnName?>>
) : UnaryPhysicalOperatorNode() {
    init {
        /* Sanity check. */
        if (this.fields.isEmpty()) {
            throw QueryException.QuerySyntaxException("Projection of type $type must specify at least one column.")
        }
    }

    /** The [ProjectionLogicalOperatorNode] maps the [ColumnDef] of its input to the output specified by [fields]. */
    override val columns: Array<ColumnDef<*>>
        get() {
            return when (type) {
                Projection.SELECT,
                Projection.SELECT_DISTINCT -> {
                    return this.fields.map { f ->
                        val column = this.input.columns.find { c -> c == f.first } ?: throw QueryException.QueryBindException("Column with name $f could not be found on input.")
                        column.copy(name = f.second ?: column.name)
                    }.toTypedArray()
                }
                Projection.EXISTS -> {
                    val alias = fields.first().second
                    val name = alias ?:
                    (this.input.columns.first().name.entity()
                        ?.column(Projection.COUNT.label())
                        ?: Name.ColumnName(Projection.COUNT.label()))
                    arrayOf(ColumnDef(name, Type.Boolean, false))
                }
                Projection.COUNT,
                Projection.COUNT_DISTINCT -> {
                    val alias = fields.first().second
                    val name = alias ?:
                    (this.input.columns.first().name.entity()
                        ?.column(Projection.COUNT.label())
                        ?: Name.ColumnName(Projection.COUNT.label()))
                    arrayOf(ColumnDef(name, Type.Long, false))
                }
                Projection.MAX,
                Projection.MIN,
                Projection.SUM,
                Projection.MEAN -> {
                    return this.fields.map { f ->
                        val column = this.input.columns.find { c -> c == f.first } ?: throw QueryException.QueryBindException("Column with name $f could not be found on input.")
                        if (!column.type.numeric) throw QueryException.QueryBindException("Projection of type $type can only be applied to numeric column, which $column isn't.")
                        column.copy(name = f.second ?: column.name)
                    }.toTypedArray()
                }
            }
        }

    override val outputSize: Long
        get() = this.input.outputSize

    override val cost: Cost
        get() = Cost(cpu = this.outputSize * this.fields.size * Cost.COST_MEMORY_ACCESS)

    override fun copy() = ProjectionPhysicalOperatorNode(this.type, this.fields)
    override fun toOperator(tx: TransactionContext, ctx: QueryContext) = when (this.type) {
        Projection.SELECT -> SelectProjectionOperator(this.input.toOperator(tx, ctx), this.fields)
        Projection.SELECT_DISTINCT -> TODO()
        Projection.COUNT -> CountProjectionOperator(this.input.toOperator(tx, ctx))
        Projection.COUNT_DISTINCT -> TODO()
        Projection.EXISTS -> ExistsProjectionOperator(this.input.toOperator(tx, ctx))
        Projection.SUM -> SumProjectionOperator(this.input.toOperator(tx, ctx), this.fields)
        Projection.MAX -> MaxProjectionOperator(this.input.toOperator(tx, ctx), this.fields)
        Projection.MIN -> MinProjectionOperator(this.input.toOperator(tx, ctx), this.fields)
        Projection.MEAN -> MeanProjectionOperator(this.input.toOperator(tx, ctx), this.fields)
    }

    /**
     * Calculates and returns the digest for this [ProjectionPhysicalOperatorNode].
     *
     * @return Digest for this [ProjectionPhysicalOperatorNode]e
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.type.hashCode()
        result = 31L * result + this.fields.hashCode()
        return result
    }
}