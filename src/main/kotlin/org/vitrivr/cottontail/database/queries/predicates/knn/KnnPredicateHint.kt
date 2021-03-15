package org.vitrivr.cottontail.database.queries.predicates.knn

import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.predicates.PredicateHint
import org.vitrivr.cottontail.model.basics.Name

/**
 * A [PredicateHint] that only affects [KnnPredicate]s. These types of hints can help the query
 * planner to make an informed decision regarding the choice of [Index]
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class KnnPredicateHint : PredicateHint {
    /**
     * A [KnnPredicateHint] that indicates, that no [Index] should be used.
     */
    object NoIndexPredicateHint : KnnPredicateHint() {
        override fun satisfies(index: Index): Boolean = false
    }

    /**
     * A [KnnPredicateHint] that indicates, whether the usage of inexact [Index]es are allowed.
     *
     * @param allow True if inexact [Index]es should be allowed, false otherwise.
     */
    data class AllowInexactHint(val allow: Boolean = false) : KnnPredicateHint() {
        override fun satisfies(index: Index): Boolean = (this.allow || !index.type.inexact)
    }

    /**
     * A [KnnPredicateHint] that indicates, what [IndexType] of [Index] should be used.
     *
     * @param type The [IndexType] of the [Index] that should be used for execution.
     */
    data class IndexTypeHint(val type: IndexType) : KnnPredicateHint() {
        override fun satisfies(index: Index): Boolean = index.type == type
    }

    /**
     * A [KnnPredicateHint] that indicates, what [Index] should be used (identified by [Name.IndexName]). This [KnnPredicateHint]
     * enables the user to specify runtime parameters that will be made available to the [Index] during runtime.
     *
     * @param name The [Name.IndexName] of the [Index] that should be used for execution.
     * @param parameters The parameters that should be given to the [Index]
     */
    data class IndexNameHint(val name: Name.IndexName, val parameters: Map<String, String>? = null) : KnnPredicateHint() {
        override fun satisfies(index: Index): Boolean = index.name == name
    }

    /**
     * A [KnnPredicateHint] that indicates, that kNN should be executed in a parallel fashion and specifies the desired
     * parallelism. Implicitly enforces execution without [Index]!
     *
     * @param min The lower bound for parallelism.
     * @param max The upper bound for parallelism.
     */
    data class ParallelKnnHint(val min: Int, val max: Int) : KnnPredicateHint() {
        init {
            require(this.min > 0) { "Desired parallelism of zero is not allowed. Minimum must be one (min = $min, max = $max)." }
            require(this.max >= this.min) { "Upper bound for desired parallelism must be greater or equal than lower bound (min = $min, max = $max)." }
        }

        override fun satisfies(index: Index): Boolean = true
    }
}