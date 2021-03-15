package org.vitrivr.cottontail.database.queries.sort

/**
 * Enumeration that describes the sort order.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class SortOrder(val sign: Int) {
    ASCENDING(1),
    DESCENDING(-1);

    /**
     * Allows for multiplication with other int.
     */
    operator fun times(other: Int): Int = this.sign * other
}