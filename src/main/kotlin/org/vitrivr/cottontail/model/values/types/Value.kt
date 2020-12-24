package org.vitrivr.cottontail.model.values.types

import java.util.*

/**
 * This is an abstraction over the existing primitive types provided by Kotlin. It allows for the
 * advanced type system implemented by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
interface Value : Comparable<Value> {
    companion object {
        /** Internal, [SplittableRandom] instance used for generation of random [Value]s. */
        @JvmStatic
        val RANDOM = SplittableRandom(System.currentTimeMillis())
    }

    /** Size of this [Value]. */
    val logicalSize: Int

    /**
     * Compares two [Value]s. Returns true, if they are equal, and false otherwise.
     *
     * TODO: This method is required because it is currently not possible to override
     * equals() in Kotlin inline classes. Once this changes, this method should be removed.
     *
     * @param other Value to compare to.
     * @return true if equal, false otherwise.
     */
    fun isEqual(other: Value): Boolean
}