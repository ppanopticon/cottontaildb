package org.vitrivr.cottontail.database.queries.predicates

import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.model.basics.Name

/**
 * A hint for a [Predicate] that can be used by the query optimizer to make appropriate decisions.
 *
 * The query optimizer will make the best effort honor the hint specified in a [PredicateHint].
 * However,  it is at the discretion of the optimiser and the execution engine, whether a given
 * hint can and will be honored.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
sealed class PredicateHint

/**
 * A [PredicateHint] that only affects [KnnPredicate]s
 *
 *
 * @author Ralph Gasser
 * @version 1.0
 */
sealed class KnnPredicateHint : PredicateHint() {
    /**
     * A [PredicateHint] that indicates, that usage of inexact indexes are allowed.
     */
    object KnnInexactPredicateHint : KnnPredicateHint()

    /**
     * A [PredicateHint] that indicates, that no index should be used.
     */
    object KnnNoIndexPredicateHint : KnnPredicateHint()

    /**
     * A [PredicateHint] that indicates, what [IndexType] should be used
     */
    data class KnnIndexTypePredicateHint(val type: IndexType) : KnnPredicateHint()

    /**
     * A [PredicateHint] that indicates, what [Index] should be used (identified by name).
     */
    data class KnnIndexNamePredicateHint(val name: Name.IndexName) : KnnPredicateHint()

    /**
     * A [PredicateHint] that indicates, how many threads should be used for execution.
     */
    data class KnnParallelismPredicateHint(val min: Int, val max: Int) : KnnPredicateHint()
}