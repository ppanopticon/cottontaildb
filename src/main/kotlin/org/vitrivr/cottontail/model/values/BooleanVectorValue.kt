package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.model.values.types.NumericValue
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.extensions.toInt
import java.util.*

/**
 * This is an abstraction over a [BooleanArray] and it represents a vector of [Boolean]s.
 *
 * @author Ralph Gasser
 * @version 1.3.1
 */
inline class BooleanVectorValue(val data: BooleanArray) : RealVectorValue<Int> {

    companion object {
        /**
         * Generates a [IntVectorValue] of the given size initialized with random numbers.
         *
         * @param size Size of the new [IntVectorValue]
         * @param rnd A [SplittableRandom] to generate the random numbers.
         * @return Random [BooleanVectorValue] of size [size]
         */
        fun random(size: Int, rnd: SplittableRandom = Value.RANDOM) = BooleanVectorValue(BooleanArray(size) { rnd.nextBoolean() })

        /**
         * Generates a [IntVectorValue] of the given size initialized with ones.
         *
         * @param size Size of the new [IntVectorValue]
         */
        fun one(size: Int) = BooleanVectorValue(BooleanArray(size) { true })

        /**
         * Generates a [IntVectorValue] of the given size initialized with zeros.
         *
         * @param size Size of the new [IntVectorValue]
         */
        fun zero(size: Int) = BooleanVectorValue(BooleanArray(size))
    }

    constructor(input: List<Number>) : this(BooleanArray(input.size) { input[it].toInt() == 1 })
    constructor(input: Array<Number>) : this(BooleanArray(input.size) { input[it].toInt() == 1 })
    constructor(input: Array<Boolean>) : this(BooleanArray(input.size) { input[it] })

    override val logicalSize: Int
        get() = data.size

    override fun compareTo(other: Value): Int {
        TODO("Not yet implemented")
    }


    /**
     * Returns the indices of this [BooleanVectorValue].
     *
     * @return The indices of this [BooleanVectorValue]
     */
    override val indices: IntRange
        get() = this.data.indices

    /**
     * Returns the i-th entry of  this [BooleanVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): IntValue = IntValue(this.data[i].toInt())

    /**
     * Returns the i-th entry of  this [BooleanVectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int) = this.data[i]

    /**
     * Returns true, if this [BooleanVectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [BooleanVectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.indices.all { !this.data[it] }

    /**
     * Returns true, if this [BooleanVectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [BooleanVectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.indices.all { this.data[it] }

    /**
     * Creates and returns a copy of this [BooleanVectorValue].
     *
     * @return Exact copy of this [BooleanVectorValue].
     */
    override fun copy(): BooleanVectorValue = BooleanVectorValue(this.data.copyOf())

    override fun plus(other: VectorValue<*>): VectorValue<Int> = when (other) {
        is BooleanVectorValue -> IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() + other.data[it].toInt())
        })
        is IntVectorValue -> IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() + other.data[it])
        })
        else -> IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() + other[it].asInt().value)
        })
    }

    override fun minus(other: VectorValue<*>): VectorValue<Int> = when (other) {
        is BooleanVectorValue -> IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() - other.data[it].toInt())
        })
        is IntVectorValue -> IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() - other.data[it])
        })
        else -> IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() - other[it].asInt().value)
        })
    }

    override fun times(other: VectorValue<*>): VectorValue<Int> = when (other) {
        is BooleanVectorValue -> IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() * other.data[it].toInt())
        })
        is IntVectorValue -> IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() * other.data[it])
        })
        else -> IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() * other[it].asInt().value)
        })
    }

    override fun div(other: VectorValue<*>): VectorValue<Int> = when (other) {
        is BooleanVectorValue -> IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() / other.data[it].toInt())
        })
        is IntVectorValue -> IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() / other.data[it])
        })
        else -> IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() / other[it].asInt().value)
        })
    }

    override fun plus(other: NumericValue<*>): VectorValue<Int> = IntVectorValue(IntArray(this.logicalSize) {
        (this.data[it].toInt() + other.asInt().value)
    })

    override fun minus(other: NumericValue<*>): VectorValue<Int> = IntVectorValue(IntArray(this.logicalSize) {
        (this.data[it].toInt() - other.asInt().value)
    })

    override fun times(other: NumericValue<*>): VectorValue<Int> = IntVectorValue(IntArray(this.logicalSize) {
        (this.data[it].toInt() * other.asInt().value)
    })

    override fun div(other: NumericValue<*>): VectorValue<Int> = IntVectorValue(IntArray(this.logicalSize) {
        (this.data[it].toInt() / other.asInt().value)
    })

    override fun pow(x: Int): DoubleVectorValue = if (x == 0) {
        DoubleVectorValue.one(this.data.size)
    } else {
        DoubleVectorValue(DoubleArray(this.data.size) {
            if (this.data[it]) {
                1.0
            } else {
                0.0
            }
        })
    }

    override fun sqrt(): DoubleVectorValue = DoubleVectorValue(DoubleArray(this.data.size) {
        if (this.data[it]) {
            1.0
        } else {
            0.0
        }
    })

    override fun abs(): RealVectorValue<Int> = this.copy()

    override fun sum(): DoubleValue = DoubleValue(this.data.sumByDouble {
        if (it) {
            1.0
        } else {
            0.0
        }
    })

    override fun norm2(): DoubleValue {
        var sum = 0.0
        for (i in this.data) {
            if (i) sum += 1.0
        }
        return DoubleValue(kotlin.math.sqrt(sum))
    }

    override fun dot(other: VectorValue<*>): DoubleValue = when (other) {
        is BooleanVectorValue -> {
            var sum = 0.0
            for (i in this.data.indices) {
                if (this.data[i] && other.data[i]) sum += 1.0
            }
            DoubleValue(sum)
        }
        else -> {
            var sum = 0.0
            for (i in this.data.indices) {
                if (this.data[i]) sum += other[i].asDouble().value
            }
            DoubleValue(sum)
        }
    }

    override fun hamming(other: VectorValue<*>): IntValue = when (other) {
        is BooleanVectorValue -> {
            var sum = 0
            val start = Arrays.mismatch(this.data, other.data)
            for (i in start until other.data.size) {
                if (this.data[i] != other.data[i]) {
                    sum += 1
                }
            }
            IntValue(sum)
        }
        else -> super.hamming(other).asInt()
    }
}