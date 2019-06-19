package ch.unibas.dmi.dbis.cottontail.database.queries

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException

/**
 * Formalizes a [Projection] operation in the Cottontail DB query execution engine.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
data class Projection(val type: ProjectionType = ProjectionType.SELECT, val columns: Array<ColumnDef<*>>, val fields: Map<String,String?>) {

    init {
        /* Sanity check. */
        when (type) {
            ProjectionType.SELECT -> if (columns.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify at least one column.")
            }
            ProjectionType.MAX -> if (columns.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify a column.")
            } else if (!columns.first().type.numeric) {
                throw QueryException.QueryBindException("Projection of type $type can only be applied on a numeric column, which ${columns.first().name} is not.")
            }
            ProjectionType.MIN -> if (columns.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify a column.")
            } else if (!columns.first().type.numeric) {
                throw QueryException.QueryBindException("Projection of type $type can only be applied to a numeric column, which ${columns.first().name} is not.")
            }
            ProjectionType.SUM -> if (columns.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify a column.")
            } else if (!columns.first().type.numeric) {
                throw QueryException.QueryBindException("Projection of type $type can only be applied to a numeric column, which ${columns.first().name} is not.")
            }
            ProjectionType.MEAN -> if (columns.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify a column.")
            } else if (!columns.first().type.numeric) {
                throw QueryException.QueryBindException("Projection of type $type can only be applied to a numeric column, which ${columns.first().name} is not.")
            }
            else -> {}
        }
    }


    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Projection

        if (type != other.type) return false
        if (!columns.contentEquals(other.columns)) return false
        if (fields != other.fields) return false

        return true
    }

    override fun hashCode(): Int {
        var result = type.hashCode()
        result = 31 * result + columns.contentHashCode()
        result = 31 * result + fields.hashCode()
        return result
    }
}

/**
 * The type [Projection]
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
enum class ProjectionType {
    SELECT, COUNT, EXISTS, SUM, MAX, MIN, MEAN
}