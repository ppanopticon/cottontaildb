package ch.unibas.dmi.dbis.cottontail.database.queries

import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.values.Value

/**
 * List of query [ComparisonOperator]s supported by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
enum class ComparisonOperator {
    EQUAL, /* One entry on right-hand side required! */
    NOT_EQUAL, /* One entry on right-hand side required! */
    GREATER, /* One entry on right-hand side required! */
    LESS, /* One entry on right-hand side required! */
    GEQUAL, /* One entry on right-hand side required! */
    LEQUAL, /* One entry on right-hand side required! */
    LIKE, /* One entry on right-hand side required! */
    NOT_LIKE,  /* One entry on right-hand side required! */
    IN, /* One to n entries on right-hand side required! */
    NOT_IN, /* One to n entries on right-hand side required! */
    BETWEEN, /* Two entries on right-hand side required! */
    NOT_BETWEEN, /* Two entries on right-hand side required! */
    ISNULL, /* No right-hand side required! */
    ISNOTNULL; /* No right-hand side required! */

    /**
     * Matches the left hand side to the right hand side given this [ComparisonOperator].
     *
     * @param left Left-hand side of the operator.
     * @param right Right-hand side of the operator.
     * @return True on match, false otherwise.
     */
    fun match(left: Value<*>?, right: Collection<Value<*>>) : Boolean = when {
        this == EQUAL && left != null -> left == right.first()
        this == NOT_EQUAL && left != null -> left != right.first()
        this == GREATER && left != null -> left > right.first()
        this == LESS && left != null -> left < right.first()
        this == GEQUAL && left != null -> left >= right.first()
        this == LEQUAL && left != null -> left <= right.first()
        this == IN && left != null -> right.contains(left)
        this == NOT_IN && left != null -> !right.contains(left)
        this == BETWEEN && left is Number -> left >= right.first() && left <= right.last()
        this == NOT_BETWEEN && left is Number -> left < right.first() || left > right.last()
        this == ISNULL -> left == null
        this == ISNOTNULL -> left != null
        else -> throw QueryException("Unknown operator '$this' or incompatible type!")
    }
}