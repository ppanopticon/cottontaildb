package org.vitrivr.cottontail.utilities.extensions

import java.util.*

/**
 * Generates and returns a random [Short]. Invoking this method is equivalent to invoking [SplittableRandom.nextInt]
 *
 * @return Random [Short]
 */
fun SplittableRandom.nextShort() = this.nextInt(Short.MIN_VALUE.toInt(), Short.MAX_VALUE.toInt()).toShort()

/**
 * Generates and returns a random [Byte]. Invoking this method is equivalent to invoking [SplittableRandom.nextInt]
 *
 * @return Random [Byte]
 */
fun SplittableRandom.nextByte() = this.nextInt(Byte.MIN_VALUE.toInt(), Byte.MAX_VALUE.toInt()).toByte()

/**
 * Generates and returns a random [Float]. Invoking this method is equivalent to invoking [SplittableRandom.nextDouble]
 *
 * @return Random [Float]
 */
fun SplittableRandom.nextFloat() = this.nextDouble().toFloat()