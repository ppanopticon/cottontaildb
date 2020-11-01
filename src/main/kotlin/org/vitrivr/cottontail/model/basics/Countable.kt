package org.vitrivr.cottontail.model.basics


/**
 * An objects that holds values and allows for counting them.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
interface Countable {
    /**
     * Returns the number of entries in this [Countable].
     *
     * @return The number of entries in this [Countable].
     */
    fun count(): Long

    /**
     * Returns the maximum [TupleId] for this [Countable].
     *
     * @return The maximum [TupleId] for this [Countable].
     */
    fun maxTupleId(): TupleId
}