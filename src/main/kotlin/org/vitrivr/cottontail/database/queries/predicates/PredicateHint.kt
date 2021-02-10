package org.vitrivr.cottontail.database.queries.predicates

import org.vitrivr.cottontail.database.index.Index

/**
 * A hint for a [Predicate] that can be used by the query optimizer to make appropriate decisions.
 *
 * The query optimizer will make the best effort honor the hint specified in a [PredicateHint].
 * However,  it is at the discretion of the optimiser and the execution engine, whether a given
 * hint can and will be honored.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
interface PredicateHint {
    /**
     * Checks if the provided [Index] satisfies this [PredicateHint].
     *
     * @param index The [Index] to check.
     * @return True, if the [Index] satisfies this [PredicateHint]
     */
    fun satisfies(index: Index): Boolean
}