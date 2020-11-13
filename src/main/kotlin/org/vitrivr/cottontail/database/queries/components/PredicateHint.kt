package org.vitrivr.cottontail.database.queries.predicates

import org.vitrivr.cottontail.database.index.Index
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
 * @version 1.0.2
 */
sealed class PredicateHint

/**
 * A [PredicateHint] that only affects [KnnPredicate]s. These types of hints can help the query planner
 * to make an informed decision regarding the choice of [Index]
 *
 * @author Ralph Gasser
 * @version 1.0.2
 */
sealed class KnnPredicateHint : PredicateHint() {

    /**
     * Checks if the provided [Index] satisfies this [KnnPredicateHint].
     *
     * @param index The [Index] to check.
     * @return True, if the [Index] satisfies this [KnnPredicateHint]
     */
    abstract fun satisfies(index: Index): Boolean

    /**
     * A [KnnPredicateHint] that indicates, that no [Index] should be used.
     */
    object NoIndexKnnPredicateHint : KnnPredicateHint() {
        override fun satisfies(index: Index): Boolean = false
    }

    /**
     * A [KnnPredicateHint] that indicates, whether the usage of inexact [Index]es are allowed.
     *
     * @param allow True if inexact [Index]es should be allowed, false otherwise.
     */
    data class AllowInexactKnnPredicateHint(val allow: Boolean = false) : KnnPredicateHint() {
        override fun satisfies(index: Index): Boolean = (this.allow || !index.type.inexact)
    }

    /**
     * A [KnnPredicateHint] that indicates, what [IndexType] of [Index] should be used.
     *
     * @param type The [IndexType] of the [Index] that should be used for execution.
     */
    data class IndexTypeKnnPredicateHint(val type: IndexType) : KnnPredicateHint() {
        override fun satisfies(index: Index): Boolean = index.type == type
    }

    /**
     * A [KnnPredicateHint] that indicates, what [Index] should be used (identified by [Name.IndexName]). This [KnnPredicateHint]
     * enables the user to specify runtime parameters that will be made available to the [Index] during runtime.
     *
     * @param name The [Name.IndexName] of the [Index] that should be used for execution.
     * @param parameters The parameters that should be given to the [Index]
     */
    data class IndexNameKnnPredicateHint(val name: Name.IndexName, val parameters: Map<String, String>? = null) : KnnPredicateHint() {
        override fun satisfies(index: Index): Boolean = index.name == name
    }

    /**
     * A [KnnPredicateHint] that indicates, that kNN should be executed in a parallel fashion and specifies the desired
     * parallelism. Implicitly enforces execution without [Index]!
     *
     * @param min The lower bound for parallelism.
     * @param max The upper bound for parallelism.
     */
    data class ParallelKnnPredicateHint(val min: Int, val max: Int) : KnnPredicateHint() {
        init {
            require(this.min > 0) { "Desired parallelism of zero is not allowed. Minimum must be one (min = $min, max = $max)." }
            require(this.max >= this.min) { "Upper bound for desired parallelism must be greater or equal than lower bound (min = $min, max = $max)." }
        }

        override fun satisfies(index: Index): Boolean = true
    }
}