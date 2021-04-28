package org.vitrivr.cottontail.database.general

/**
 * Signifies the type of [Placeholder].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class PlaceholderType {
    CREATED,

    /** [DBO] was just created and cannot be used yet. */
    DROPPED,

    /** [DBO] was just dropped and cannot be used anymore. */
    BROKEN
    /** [DBO] is broken and cannot be used. */
}