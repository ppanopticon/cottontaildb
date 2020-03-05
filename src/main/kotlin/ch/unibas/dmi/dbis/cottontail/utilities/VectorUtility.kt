package ch.unibas.dmi.dbis.cottontail.utilities

import ch.unibas.dmi.dbis.cottontail.model.values.DoubleVectorValue
import ch.unibas.dmi.dbis.cottontail.model.values.FloatVectorValue
import ch.unibas.dmi.dbis.cottontail.model.values.IntVectorValue
import java.util.*

object VectorUtility {

    /** The random number generator used for vector generation. */
    private val random = SplittableRandom()

    /**
     * Generates a random [IntArray] of the given size.
     *
     * @param size The size of the random vector.
     */
    fun randomIntVector(size: Int) : IntVectorValue {
        val vec = IntArray(size)
        for (i in vec.indices) {
            vec[i] = random.nextInt()
        }
        return IntVectorValue(vec)
    }

    /**
     * Generates a random [LongArray] of the given size.
     *
     * @param size The size of the random vector.
     */
    fun randomLongVector(size: Int) : LongArray {
        val vec = LongArray(size)
        for (i in vec.indices) {
            vec[i] = random.nextLong()
        }
        return vec
    }

    /**
     * Generates a random [FloatArray] of the given size.
     *
     * @param size The size of the random vector.
     */
    fun randomFloatVector(size: Int) : FloatVectorValue {
        val vec = FloatArray(size)
        for (i in vec.indices) {
            vec[i] = Float.fromBits(this.random.nextInt())
        }
        return FloatVectorValue(vec)
    }

    /**
     * Generates a random [DoubleVectorValue] of the given size.
     *
     * @param size The size of the random vector.
     */
    fun randomDoubleVector(size: Int) : DoubleVectorValue {
        val vec = DoubleArray(size)
        for (i in vec.indices) {
            vec[i] = random.nextDouble()
        }
        return DoubleVectorValue(vec)
    }

    /**
     * Generates a sequence of random [IntArray] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomIntVectorSequence(size: Int, items: Int = Int.MAX_VALUE) : Iterator<IntVectorValue> = object: Iterator<IntVectorValue> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): IntVectorValue {
            this.left -= 1
            return randomIntVector(size)
        }
    }

    /**
     * Generates a sequence of random [FloatArray] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomLongVectorSequence(size: Int, items: Int = Int.MAX_VALUE) : Iterator<LongArray> = object: Iterator<LongArray> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): LongArray {
            this.left -= 1
            return randomLongVector(size)
        }
    }

    /**
     * Generates a sequence of random [FloatVectorValue] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomFloatVectorSequence(size: Int, items: Int = Int.MAX_VALUE) : Iterator<FloatVectorValue> = object: Iterator<FloatVectorValue> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): FloatVectorValue {
            this.left -= 1
            return randomFloatVector(size)
        }
    }

    /**
     * Generates a sequence of random [DoubleVectorValue] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomDoubleVectorSequence(size: Int, items: Int = Int.MAX_VALUE) : Iterator<DoubleVectorValue> = object: Iterator<DoubleVectorValue> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): DoubleVectorValue {
            this.left -= 1
            return randomDoubleVector(size)
        }
    }
}