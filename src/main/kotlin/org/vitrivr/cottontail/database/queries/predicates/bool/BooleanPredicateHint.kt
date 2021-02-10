package org.vitrivr.cottontail.database.queries.predicates.bool

import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.predicates.PredicateHint
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicateHint
import org.vitrivr.cottontail.model.basics.Name

/**
 * A [PredicateHint] that only affects [BooleanPredicate]s. These types of hints can help the query
 * planner to make an informed decision regarding the choice of [Index]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class BooleanPredicateHint : PredicateHint {
    /**
     * A [BooleanPredicateHint] that indicates, that no [Index] should be used.
     */
    object NoIndexHint : BooleanPredicateHint() {
        override fun satisfies(index: Index): Boolean = false
    }

    /**
     * A [BooleanPredicateHint] that indicates, what [IndexType] of [Index] should be used.
     *
     * @param type The [IndexType] of the [Index] that should be used for execution.
     */
    data class IndexTypeHint(val type: IndexType) : BooleanPredicateHint() {
        override fun satisfies(index: Index): Boolean = index.type == type
    }

    /**
     * A [BooleanPredicateHint] that indicates, what [Index] should be used (identified by [Name.IndexName]).
     * This [KnnPredicateHint] enables the user to specify runtime parameters that will be made
     * available to the [Index] during runtime.
     *
     * @param name The [Name.IndexName] of the [Index] that should be used for execution.
     * @param parameters The parameters that should be given to the [Index]
     */
    data class IndexNameHint(
        val name: Name.IndexName,
        val parameters: Map<String, String>? = null
    ) : BooleanPredicateHint() {
        override fun satisfies(index: Index): Boolean = index.name == name
    }
}