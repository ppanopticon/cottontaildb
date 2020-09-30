package org.vitrivr.cottontail.database.queries.components

/**
 * Enumeration of all [Projection] operations supported by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
enum class Projection(val aggregating: Boolean) {
    SELECT(false),
    SELECT_DISTINCT(false),
    COUNT(true),
    COUNT_DISTINCT(true),
    EXISTS(true),
    SUM(true),
    MAX(true),
    MIN(true),
    MEAN(true)
}