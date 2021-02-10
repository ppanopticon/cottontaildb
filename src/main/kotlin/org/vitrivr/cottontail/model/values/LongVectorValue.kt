package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.types.NumericValue
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
import java.util.*
import kotlin.math.pow

/**
 * This is an abstraction over a [FloatArray] and it represents a vector of [Float]s.
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
inline class LongVectorValue(val data: LongArray) : RealVectorValue<Long> {

    companion object {
        /**
         * Generates a [LongVectorValue] of the given size initialized with random numbers.
         *
         * @param size Size of the new [LongVectorValue]
         * @param rnd A [SplittableRandom] to generate the random numbers.
         */
        fun random(size: Int, rnd: SplittableRandom = Value.RANDOM) = LongVectorValue(LongArray(size) { rnd.nextLong() })

        /**
         * Generates a [LongVectorValue] of the given size initialized with ones.
         *
         * @param size Size of the new [LongVectorValue]
         */
        fun one(size: Int) = LongVectorValue(LongArray(size) { 1L })

        /**
         * Generates a [IntVectorValue] of the given size initialized with zeros.
         *
         * @param size Size of the new [LongVectorValue]
         */
        fun zero(size: Int) = LongVectorValue(LongArray(size))
    }

    constructor(input: List<Number>) : this(LongArray(input.size) { input[it].toLong() })
    constructor(input: Array<Number>) : this(LongArray(input.size) { input[it].toLong() })

    /** The logical size of this [LongVectorValue]. */
    override val logicalSize: Int
        get() = this.data.size

    /** The [Type] of this [LongVectorValue]. */
    override val type: Type<*>
        get() = Type.LongVector(this.logicalSize)

    /**
     * Checks for equality between this [LongVectorValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [LongVectorValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean =
        (other is LongVectorValue) && (this.data.contentEquals(other.data))

    /**
     * Returns the indices of this [LongVectorValue].
     *
     * @return The indices of this [LongVectorValue]
     */
    override val indices: IntRange
        get() = this.data.indices

    /**
     * Returns the i-th entry of  this [LongVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int) = LongValue(this.data[i])

    /**
     * Returns the i-th entry of  this [LongVectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int) = this.data[i] != 0L

    /**
     * Returns true, if this [LongVectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [LongVectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.data.all { it == 0L }

    /**
     * Returns true, if this [LongVectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [LongVectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.data.all { it == 1L }

    /**
     * Creates and returns a copy of this [LongVectorValue].
     *
     * @return Exact copy of this [LongVectorValue].
     */
    override fun copy(): LongVectorValue = LongVectorValue(this.data.copyOf(this.logicalSize))

    /**
     * Creates and returns a new instance of [LongVectorValue] of the same size.
     *
     * @return New instance of [LongVectorValue]
     */
    override fun new(): LongVectorValue = LongVectorValue(LongArray(this.data.size))

    override fun plus(other: VectorValue<*>): LongVectorValue = when (other) {
        is LongVectorValue -> LongVectorValue(LongArray(this.data.size) {
            (this.data[it] + other.data[it])
        })
        else -> LongVectorValue(LongArray(this.data.size) {
            (this.data[it] + other[it].asLong().value)
        })
    }

    override fun minus(other: VectorValue<*>): LongVectorValue = when (other) {
        is LongVectorValue -> LongVectorValue(LongArray(this.data.size) {
            (this.data[it] - other.data[it])
        })
        else -> LongVectorValue(LongArray(this.data.size) {
            (this.data[it] - other[it].asLong().value)
        })
    }

    override fun times(other: VectorValue<*>): LongVectorValue = when (other) {
        is LongVectorValue -> LongVectorValue(LongArray(this.data.size) {
            (this.data[it] * other.data[it])
        })
        else -> LongVectorValue(LongArray(this.data.size) {
            (this.data[it] * other[it].asLong().value)
        })
    }

    override fun div(other: VectorValue<*>): LongVectorValue = when (other) {
        is LongVectorValue -> LongVectorValue(LongArray(this.data.size) {
            (this.data[it] / other.data[it])
        })
        else -> LongVectorValue(LongArray(this.data.size) {
            (this.data[it] / other[it].asLong().value)
        })
    }

    override fun plus(other: NumericValue<*>): LongVectorValue {
        val otherAsLong = other.asLong().value
        return LongVectorValue(LongArray(this.logicalSize) {
            (this.data[it] + otherAsLong)
        })
    }

    override fun minus(other: NumericValue<*>): LongVectorValue {
        val otherAsLong = other.asLong().value
        return LongVectorValue(LongArray(this.logicalSize) {
            (this.data[it] - otherAsLong)
        })
    }

    override fun times(other: NumericValue<*>): LongVectorValue {
        val otherAsLong = other.asLong().value
        return LongVectorValue(LongArray(this.logicalSize) {
            (this.data[it] * otherAsLong)
        })
    }

    override fun div(other: NumericValue<*>): LongVectorValue {
        val otherAsLong = other.asLong().value
        return LongVectorValue(LongArray(this.logicalSize) {
            (this.data[it] / otherAsLong)
        })
    }

    override fun pow(x: Int) = DoubleVectorValue(DoubleArray(this.data.size) {
        this.data[it].toDouble().pow(x)
    })

    override fun sqrt() = DoubleVectorValue(DoubleArray(this.data.size) {
        kotlin.math.sqrt(this.data[it].toDouble())
    })

    override fun abs() = LongVectorValue(LongArray(this.logicalSize) {
        kotlin.math.abs(this.data[it])
    })

    override fun sum(): DoubleValue = DoubleValue(this.data.map { it.toDouble() }.sum())

    override fun norm2(): DoubleValue {
        var sum = 0.0
        for (i in this.indices) {
            sum += this[i].value * this[i].value
        }
        return DoubleValue(kotlin.math.sqrt(sum))
    }

    override fun dot(other: VectorValue<*>): DoubleValue {
        var sum = 0.0
        for (i in this.indices) {
            sum += other[i].value.toInt() * this[i].value
        }
        return DoubleValue(sum)
    }

    override fun l1(other: VectorValue<*>): DoubleValue = when (other) {
        is LongVectorValue -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum += kotlin.math.abs(this.data[i] - other.data[i])
            }
            DoubleValue(sum)
        }
        else -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum += kotlin.math.abs(this.data[i] - other[i].value.toLong())
            }
            DoubleValue(sum)
        }
    }

    override fun l2(other: VectorValue<*>): DoubleValue = when (other) {
        is LongVectorValue -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum += (this.data[i] - other.data[i]).toDouble().pow(2)
            }
            DoubleValue(kotlin.math.sqrt(sum))
        }
        else -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum += (this.data[i] - other[i].value.toLong()).toDouble().pow(2)
            }
            DoubleValue(kotlin.math.sqrt(sum))
        }
    }

    override fun lp(other: VectorValue<*>, p: Int): DoubleValue = when (other) {
        is LongVectorValue -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum += (this.data[i] - other.data[i]) * (this.data[i] - other.data[i]).toDouble().pow(p)
            }
            DoubleValue(sum.pow(1.0 / p))
        }
        else -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum += (this.data[i] - other[i].value.toInt()).toFloat().pow(p)
            }
            DoubleValue(sum.pow(1.0 / p))
        }
    }

    override fun hamming(other: VectorValue<*>): LongValue = when (other) {
        is LongVectorValue -> {
            var sum = 0L
            val start = Arrays.mismatch(this.data, other.data)
            for (i in start until other.data.size) {
                if (this.data[i] != other.data[i]) {
                    sum += 1L
                }
            }
            LongValue(sum)
        }
        else -> LongValue(this.data.size)
    }

    /**
     * Returns a sub vector of this [LongVectorValue] starting at the component [start] and
     * containing [length] components.
     *
     * @param start Index of the first entry of the returned vector.
     * @param length how many elements, including start, to return
     *
     * @return The [LongVectorValue] representing the sub-vector.
     */
    override fun subvector(start: Int, length: Int) =
        LongVectorValue(this.data.copyOfRange(start, start + length))
}