package org.vitrivr.cottontail.model.basics

import org.vitrivr.cottontail.database.queries.components.Predicate
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * An objects that holds [Record] values and allows for filtering & filtered scanning operation on those [Record] values.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.1
 */
interface Filterable {
    /**
     * Checks if this [Filterable] can process the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    fun canProcess(predicate: Predicate): Boolean

    /**
     * Filters this [Filterable] thereby creating and returning a new [Iterable] for all
     * the [Record]s contained in this [Filterable].
     *
     * @param predicate [Predicate] to filter [Record]s.
     * @return New [Iterable]
     *
     * @throws QueryException.UnsupportedPredicateException If predicate is not supported by this [Filterable]
     */
    @Throws(QueryException.UnsupportedPredicateException::class)
    fun filter(predicate: Predicate): CloseableIterator<Record>
}


