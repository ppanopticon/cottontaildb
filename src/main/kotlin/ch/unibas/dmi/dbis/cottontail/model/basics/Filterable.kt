package ch.unibas.dmi.dbis.cottontail.model.basics

import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException

/**
 * An objects that holds [Record] values and allows for filtering & filtered scanning operation on those [Record] values.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.0
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
     * Filters this [Filterable] thereby creating and returning a new [Filterable].
     *
     * @param predicate [Predicate] to filter [Record]s.
     * @return New [Filterable]
     *
     * @throws QueryException.UnsupportedPredicateException If predicate is not supported by data structure.
     */
    @Throws(QueryException.UnsupportedPredicateException::class)
    fun filter(predicate: Predicate): Filterable

    /**
     * Applies the provided action to each [Record] in the given range that matches the given [Predicate].
     *
     * @param from The tuple ID of the first [Record] to iterate over.
     * @param to The tuple ID of the last [Record] to iterate over.
     * @param predicate [Predicate] to filter [Record]s.
     * @param action The action that should be applied.
     *
     * @throws QueryException.UnsupportedPredicateException If predicate is not supported by data structure.
     */
    @Throws(QueryException.UnsupportedPredicateException::class)
    fun forEach(from: Long, to: Long, predicate: Predicate, action: (Record) -> Unit)

    /**
     * Applies the provided action to each [Record]  that matches the given [Predicate].
     *
     * @param predicate [Predicate] to filter [Record]s.
     * @param action The action that should be applied.
     *
     * @throws QueryException.UnsupportedPredicateException If predicate is not supported by data structure.
     */
    @Throws(QueryException.UnsupportedPredicateException::class)
    fun forEach(predicate: Predicate, action: (Record) -> Unit)

    /**
     * Applies the provided mapping function to each [Record] that matches the given [Predicate].
     *
     * @param predicate [Predicate] to filter [Record]s.
     * @param action The mapping function that should be applied.
     * @return Collection of the results of the mapping function.
     *
     * @throws QueryException.UnsupportedPredicateException If predicate is not supported by data structure.
     */
    @Throws(QueryException.UnsupportedPredicateException::class)
    fun <R> map(predicate: Predicate, action: (Record) -> R): Collection<R>

    /**
     * Applies the provided mapping function to each [Record]  in the given range that matches the given [Predicate].
     *
     * @param from The tuple ID of the first [Record] to iterate over.
     * @param to The tuple ID of the last [Record] to iterate over.
     * @param predicate [Predicate] to filter [Record]s.
     * @param action The mapping function that should be applied.
     * @return Collection of the results of the mapping function.
     *
     * @throws QueryException.UnsupportedPredicateException If predicate is not supported by data structure.
     */
    @Throws(QueryException.UnsupportedPredicateException::class)
    fun <R> map(from: Long, to: Long, predicate: Predicate, action: (Record) -> R): Collection<R>
}


