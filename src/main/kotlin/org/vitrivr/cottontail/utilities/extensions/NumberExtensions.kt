package org.vitrivr.cottontail.utilities.extensions

/**
 * Rounds this [Long] up to a multiple of the given [Long].
 *
 * @param toMultipleOf Value to round to a multiple of.
 */
fun Long.roundUp(toMultipleOf: Long): Long = (this + toMultipleOf - 1) / toMultipleOf * toMultipleOf

/**
 * Rounds this [Long] down to a multiple of the given [Long].
 *
 * @param toMultipleOf Value to round to a multiple of.
 */
fun Long.roundDown(toMultipleOf: Long): Long = this - this % toMultipleOf

/**
 * Rounds this [Int] up to a multiple of the given [Long].
 *
 * @param toMultipleOf Value to round to a multiple of.
 */
fun Int.roundUp(toMultipleOf: Int): Int = (this + toMultipleOf - 1) / toMultipleOf * toMultipleOf

/**
 * Rounds this [Int] down to a multiple of the given [Long].
 *
 * @param toMultipleOf Value to round to a multiple of.
 */
fun Int.roundDown(toMultipleOf: Int): Int = this - this % toMultipleOf

/**
 *
 */
fun Int.shift(): Int = 31 - Integer.numberOfLeadingZeros(this)
