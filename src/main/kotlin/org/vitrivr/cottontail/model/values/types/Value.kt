package org.vitrivr.cottontail.model.values.types

import java.util.*

/**
 * This is an abstraction over the existing primitive types provided by Kotlin. It allows for the
 * advanced type system implemented by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.2.1
 */
interface Value {
    companion object {
        /** Internal, [SplittableRandom] instance used for generation of random [Value]s. */
        @JvmStatic
        val RANDOM = SplittableRandom(System.currentTimeMillis())
    }

    /** Size of this [Value]. */
    val logicalSize: Int

    /**
     * Comparison operator between two values. Returns -1, 0 or 1 of other value is smaller, equal
     * or greater than this value.
     *
     * @param other Value to compare to.
     * @return -1, 0 or 1 of other value is smaller, equal or greater than this value
     */
    operator fun compareTo(other: Value): Int
}