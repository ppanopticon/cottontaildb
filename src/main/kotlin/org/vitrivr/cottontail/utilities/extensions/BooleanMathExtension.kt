package org.vitrivr.cottontail.utilities.extensions

/**
 * Converts this [Boolean] to an [Int] (true = 1, false = 0).
 *
 * @return [Int] representation of this [Boolean]
 */
fun Boolean.toByte() = if (this) 1.toByte() else 0.toByte()

/**
 * Converts this [Boolean] to an [Short] (true = 1, false = 0).
 *
 * @return [Short] representation of this [Boolean]
 */
fun Boolean.toShort() = if (this) 1.toShort() else 0.toShort()

/**
 * Converts this [Boolean] to an [Int] (true = 1, false = 0).
 *
 * @return [Int] representation of this [Boolean]
 */
fun Boolean.toInt() = if (this) 1 else 0

/**
 * Converts this [Boolean] to an [Double] (true = 1.0, false = 0.0).
 *
 * @return [Double] representation of this [Boolean]
 */
fun Boolean.toDouble() = if (this) 1.0 else 0.0

/**
 * Converts this [Boolean] to an [Float] (true = 1.0f, false = 0.0f).
 *
 * @return [Float] representation of this [Boolean]
 */
fun Boolean.toFloat() = if (this) 1.0f else 0.0f

/**
 * Converts this [Boolean] to an [Long] (true = 1L, false = 0L).
 *
 * @return [Long] representation of this [Boolean]
 */
fun Boolean.toLong() = if (this) 1L else 0L

/** Boolean plus operations for numeric values. */
/** Left-hand side. */
operator fun Boolean.plus(o: Boolean): Int = this.toInt() + o.toInt()
operator fun Boolean.plus(o: Int): Int = this.toInt() + o
operator fun Boolean.plus(o: Long): Long = this.toLong() + o
operator fun Boolean.plus(o: Float): Float = this.toFloat() + o
operator fun Boolean.plus(o: Double): Double = this.toDouble() + o

/**  Right-hand side. */
operator fun Int.plus(o: Boolean) = this + o.toInt()
operator fun Long.plus(o: Boolean) = this + o.toLong()
operator fun Double.plus(o: Boolean) = this + o.toDouble()
operator fun Float.plus(o: Boolean) = this + o.toFloat()


/**  Boolean times operations for numeric values. */
/** Left-hand side. */
operator fun Boolean.times(o: Boolean): Int = this.toInt() * o.toInt()
operator fun Boolean.times(o: Int): Int = this.toInt() * o
operator fun Boolean.times(o: Long): Long = this.toLong() * o
operator fun Boolean.times(o: Float): Float = this.toFloat() * o
operator fun Boolean.times(o: Double): Double = this.toDouble() * o

/**  Right-hand side. */
operator fun Int.times(o: Boolean) = this * o.toInt()
operator fun Long.times(o: Boolean) = this * o.toLong()
operator fun Double.times(o: Boolean) = this * o.toDouble()
operator fun Float.times(o: Boolean) = this * o.toFloat()

/** Boolean minus operations for numeric values. */
operator fun Boolean.minus(o: Boolean): Int = this.toInt() - o.toInt()
operator fun Boolean.minus(o: Int): Int = this.toInt() - o
operator fun Boolean.minus(o: Long): Long = this.toLong() - o
operator fun Boolean.minus(o: Float): Float = this.toFloat() - o
operator fun Boolean.minus(o: Double): Double = this.toDouble() - o

/**  Right-hand side. */
operator fun Int.minus(o: Boolean) = this - o.toInt()
operator fun Long.minus(o: Boolean) = this - o.toLong()
operator fun Double.minus(o: Boolean) = this - o.toDouble()
operator fun Float.minus(o: Boolean) = this - o.toFloat()

/** Boolean div operations for numeric values. */
/** Left-hand side. */
operator fun Boolean.div(o: Boolean): Int = this.toInt() / o.toInt()
operator fun Boolean.div(o: Int): Int = this.toInt() / o
operator fun Boolean.div(o: Long): Long = this.toLong() / o
operator fun Boolean.div(o: Float): Float = this.toFloat() / o
operator fun Boolean.div(o: Double): Double = this.toDouble() / o

/**  Right-hand side. */
operator fun Int.div(o: Boolean) = this / o.toInt()
operator fun Long.div(o: Boolean) = this / o.toLong()
operator fun Double.div(o: Boolean) = this / o.toDouble()
operator fun Float.div(o: Boolean) = this / o.toFloat()









