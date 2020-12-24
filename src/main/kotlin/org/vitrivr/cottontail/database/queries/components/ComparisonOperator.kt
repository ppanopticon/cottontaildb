package org.vitrivr.cottontail.database.queries.components

import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.pattern.LikePatternValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * Enumeration of query [ComparisonOperator]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class ComparisonOperator {
    EQUAL, /* One entry on right-hand side required! */
    GREATER, /* One entry on right-hand side required! */
    LESS, /* One entry on right-hand side required! */
    GEQUAL, /* One entry on right-hand side required! */
    LEQUAL, /* One entry on right-hand side required! */
    LIKE, /* One entry on right-hand side required! */
    MATCH, /* One entry on right-hand side required! */
    IN, /* One to n entries on right-hand side required! */
    BETWEEN, /* Two entries on right-hand side required! */
    ISNULL, /* No right-hand side required! */
    ISNOTNULL; /* No right-hand side required! */

    /**
     * Matches the left hand side to the right hand side given this [ComparisonOperator].
     *
     * @param left Left-hand side of the operator.
     * @param right Right-hand side of the operator.
     * @return True on match, false otherwise.
     */
    fun match(left: Value?, right: Collection<Value>): Boolean = try {
        when (this) {
            EQUAL -> left != null && left.isEqual(right.first())
            GREATER -> left != null && left > right.first()
            LESS -> left != null && left < right.first()
            GEQUAL -> left != null && left >= right.first()
            LEQUAL -> left != null && left <= right.first()
            IN -> left != null && right.contains(left)
            BETWEEN -> left != null && left >= right.first() && left <= (right.last())
            ISNULL -> left == null
            ISNOTNULL -> left != null
            LIKE -> {
                val check = right.first()
                if (left is StringValue && check is LikePatternValue) {
                    check.matches(left)
                } else {
                    throw QueryException("Incompatible operands for operator $this (left = ${left?.javaClass?.simpleName}, left = ${check::class.simpleName})!")
                }
            }
            MATCH -> throw QueryException("Operator $this requires lucene index!")
        }
    } catch (e: NoSuchElementException) {
        throw QueryException("Incompatible operands for operator $this: Right operand cannot be empty!")
    }
}