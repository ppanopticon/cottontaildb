package org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * A [UnaryLogicalOperatorNode] that represents a projection operation on a [org.vitrivr.cottontail.model.recordset.Recordset].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class ProjectionLogicalOperatorNode(
    val type: Projection = Projection.SELECT,
    val fields: List<Pair<ColumnDef<*>, Name.ColumnName?>>
) : UnaryLogicalOperatorNode() {
    init {
        /* Sanity check. */
        if (this.fields.isEmpty()) {
            throw QueryException.QuerySyntaxException("Projection of type $type must specify at least one column.")
        }
    }

    /** The [ProjectionLogicalOperatorNode] maps the [ColumnDef] of its input to the output specified by [fields]. */
    override val columns: Array<ColumnDef<*>>
        get() {
            val input = this.input ?: return emptyArray()
            return when (type) {
                Projection.SELECT,
                Projection.SELECT_DISTINCT -> {
                    return this.fields.map { f ->
                        val column = input.columns.find { c -> c == f.first } ?: throw QueryException.QueryBindException("Column with name $f could not be found on input.")
                        column.copy(name = f.second ?: column.name)
                    }.toTypedArray()
                }
                Projection.EXISTS -> {
                    val alias = fields.first().second
                    val name = alias ?: (input.columns.first().name.entity()?.column(Projection.COUNT.label()) ?: Name.ColumnName(Projection.COUNT.label()))
                    arrayOf(ColumnDef(name, Type.Boolean, false))
                }
                Projection.COUNT,
                Projection.COUNT_DISTINCT -> {
                    val alias = fields.first().second
                    val name = alias ?: (input.columns.first().name.entity()?.column(Projection.COUNT.label()) ?: Name.ColumnName(Projection.COUNT.label()))
                    arrayOf(ColumnDef(name, Type.Long,  false))
                }
                Projection.MAX,
                Projection.MIN,
                Projection.SUM,
                Projection.MEAN -> {
                    return this.fields.map { f ->
                        val column = input.columns.find { c -> c == f.first } ?: throw QueryException.QueryBindException("Column with name $f could not be found on input.")
                        if (!column.type.numeric) throw QueryException.QueryBindException("Projection of type $type can only be applied to numeric column, which $column isn't.")
                        column.copy(name = f.second ?: column.name)
                    }.toTypedArray()
                }
            }
        }

    /**
     * Returns a copy of this [ProjectionLogicalOperatorNode]
     *
     * @return Copy of this [ProjectionLogicalOperatorNode]
     */
    override fun copy(): ProjectionLogicalOperatorNode =
        ProjectionLogicalOperatorNode(this.type, this.fields)

    /**
     * Calculates and returns the digest for this [ProjectionLogicalOperatorNode].
     *
     * @return Digest for this [ProjectionLogicalOperatorNode]
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.type.hashCode()
        result = 31L * result + this.fields.hashCode()
        return result
    }
}