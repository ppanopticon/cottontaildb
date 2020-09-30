package org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection

import org.vitrivr.cottontail.database.queries.components.Projection
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalNodeExpression
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * A [UnaryLogicalNodeExpression] that represents a projection operation on a [org.vitrivr.cottontail.model.recordset.Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class ProjectionLogicalNodeExpression(val type: Projection = Projection.SELECT, val fields: List<Pair<Name.ColumnName, Name.ColumnName?>>) : UnaryLogicalNodeExpression() {
    init {
        /* Sanity check. */
        when (type) {
            Projection.SELECT -> if (this.fields.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify at least one column.")
            }
            Projection.SELECT_DISTINCT -> if (this.fields.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify at least one column.")
            }
            Projection.COUNT_DISTINCT -> if (this.fields.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify at least one column.")
            }
            Projection.MAX -> if (this.fields.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify a column.")
            }
            Projection.MIN -> if (this.fields.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify a column.")
            }
            Projection.SUM -> if (this.fields.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify a column.")
            }
            Projection.MEAN -> if (fields.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify a column.")
            }
            else -> { /* No Op. */
            }
        }
    }

    /**
     * Returns a copy of this [ProjectionLogicalNodeExpression]
     *
     * @return Copy of this [ProjectionLogicalNodeExpression]
     */
    override fun copy(): ProjectionLogicalNodeExpression = ProjectionLogicalNodeExpression(this.type, this.fields)
}