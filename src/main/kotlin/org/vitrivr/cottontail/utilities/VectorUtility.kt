package org.vitrivr.cottontail.utilities

import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.VectorValue
import java.util.*

/**
 * Utility class used to generate a stream of [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2
 */
object VectorUtility {

    /**
     * Generates a sequence of random [BooleanVectorValue] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomBoolVectorSequence(size: Int, items: Int = Int.MAX_VALUE, random: SplittableRandom = SplittableRandom()): Iterator<BooleanVectorValue> = object : Iterator<BooleanVectorValue> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): BooleanVectorValue {
            this.left -= 1
            return BooleanVectorValue.random(size, random)
        }
    }

    /**
     * Generates a sequence of random [IntVectorValue] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomIntVectorSequence(size: Int, items: Int = Int.MAX_VALUE, random: SplittableRandom = SplittableRandom()): Iterator<IntVectorValue> = object : Iterator<IntVectorValue> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): IntVectorValue {
            this.left -= 1
            return IntVectorValue.random(size, random)
        }
    }

    /**
     * Generates a sequence of random [LongVectorValue] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomLongVectorSequence(size: Int, items: Int = Int.MAX_VALUE, random: SplittableRandom = SplittableRandom()): Iterator<LongVectorValue> = object : Iterator<LongVectorValue> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): LongVectorValue {
            this.left -= 1
            return LongVectorValue.random(size, random)
        }
    }

    /**
     * Generates a sequence of random [FloatVectorValue] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomFloatVectorSequence(size: Int, items: Int = Int.MAX_VALUE, random: SplittableRandom = SplittableRandom()): Iterator<FloatVectorValue> = object : Iterator<FloatVectorValue> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): FloatVectorValue {
            this.left -= 1
            return FloatVectorValue.random(size, random)
        }
    }

    /**
     * Generates a sequence of random [DoubleVectorValue] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomDoubleVectorSequence(size: Int, items: Int = Int.MAX_VALUE, random: SplittableRandom = SplittableRandom()): Iterator<DoubleVectorValue> = object : Iterator<DoubleVectorValue> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): DoubleVectorValue {
            this.left -= 1
            return DoubleVectorValue.random(size, random)
        }
    }

    /**
     * Generates a sequence of random [Complex32VectorValue] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomComplex32VectorSequence(size: Int, items: Int = Int.MAX_VALUE, random: SplittableRandom = SplittableRandom()): Iterator<Complex32VectorValue> = object : Iterator<Complex32VectorValue> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): Complex32VectorValue {
            this.left -= 1
            return Complex32VectorValue.random(size, random)
        }
    }

    /**
     * Generates a sequence of random [Complex64VectorValue] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomComplex64VectorSequence(size: Int, items: Int = Int.MAX_VALUE, random: SplittableRandom = SplittableRandom()): Iterator<Complex64VectorValue> = object : Iterator<Complex64VectorValue> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): Complex64VectorValue {
            this.left -= 1
            return Complex64VectorValue.random(size, random)
        }
    }
}