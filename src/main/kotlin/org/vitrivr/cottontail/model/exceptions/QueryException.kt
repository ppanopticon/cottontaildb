package org.vitrivr.cottontail.model.exceptions

/**
 * [QueryException]s are thrown whenever a query cannot prepared properly. Potential reasons for
 * failing to prepare a query range from erroneous user input to programmer's errors.
 *
 * Errors that take place during query execution are not [QueryException]s.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
open class QueryException(message: String) : DatabaseException(message) {

    /**
     * [QuerySyntaxException] is thrown when the query does not adhere to some syntax requirement.
     * For example, if mandatory components of the query are missing.
     *
     * @param message Message describing the issue.
     */
    class QuerySyntaxException(message: String) : DatabaseException(message)

    /**
     * [QueryBindException] is thrown when Cottontail DB fails to bind a query to the underlying [org.vitrivr.cottontail.database.general.DBO]s.
     * This is usually the case, if [org.vitrivr.cottontail.database.schema.Schema], [org.vitrivr.cottontail.database.entity.Entity]
     * or [org.vitrivr.cottontail.database.column.Column] names are not spelt correctly.
     *
     * @param message Message describing the issue with the query.
     */
    class QueryBindException(message: String) : DatabaseException(message)

    /**
     * [QueryPlannerException] is thrown when [org.vitrivr.cottontail.database.queries.planning.CottontailQueryPlanner]
     * fails to generate a valid execution plan for the query.
     *
     * @param message Message describing the issue with the query.
     */
    class QueryPlannerException(message: String) : DatabaseException(message)

    /**
     * [UnsupportedCastException] is thrown whenever a literal value cannot be cast to the desired type.
     *
     * @param message Message describing the issue with the query.
     */
    class UnsupportedCastException(message: String) : DatabaseException(message)

    /**
     * [UnsupportedPredicateException] is thrown when a [Predicate][org.vitrivr.cottontail.database.queries.Predicate]
     * is applied that is not supported by the data structure it is applied to.
     *
     * @param message Message describing the issue with the query.
     */
    class UnsupportedPredicateException(message: String) : DatabaseException(message)
}