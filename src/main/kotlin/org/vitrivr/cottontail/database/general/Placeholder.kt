package org.vitrivr.cottontail.database.general

/**
 * A [Placeholder] for an actual [DBO]. Used in Cottontail DB to indicate, that a [DBO] is present but unusable for some reason.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface Placeholder : DBO {
    /** Signifies the type of [Placeholder] this is. */
    val type: PlaceholderType

    /**
     * Tries to initialize and actual [DBO] instance for this [Placeholder].
     * Returns [DBO] on success and throws an [Exception] otherwise.
     *
     * @return [DBO]
     * @throws [Exception] If [DBO] initialization fails.
     */
    fun initialize(): DBO

    /**
     * Tries to initialize and actual [DBO] instance for this [Placeholder].
     * Returns [DBO] on success and null otherwise.
     *
     * @return [DBO]
     */
    fun tryInitialize(): DBO? = try {
        this.initialize()
    } catch (e: Throwable) {
        null
    }
}