package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.values.types.NumericValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.RealVectorValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.model.values.types.VectorValue
import java.util.*
import kotlin.math.absoluteValue
import kotlin.math.pow

/**
 * This is an abstraction over a [DoubleArray] and it represents a vector of [Double]s.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
inline class DoubleVectorValue(val data: DoubleArray) : RealVectorValue<Double> {

    companion object {
        /**
         * Generates a [Complex32VectorValue] of the given size initialized with random numbers.
         *
         * @param size Size of the new [Complex32VectorValue]
         * @param rnd A [SplittableRandom] to generate the random numbers.
         */
        fun random(size: Int, rnd: SplittableRandom = SplittableRandom(System.currentTimeMillis())) = DoubleVectorValue(DoubleArray(size) { Double.fromBits(rnd.nextLong()) })

        /**
         * Generates a [Complex32VectorValue] of the given size initialized with ones.
         *
         * @param size Size of the new [Complex32VectorValue]
         */
        fun one(size: Int) = DoubleVectorValue(DoubleArray(size) { 1.0 })

        /**
         * Generates a [Complex32VectorValue] of the given size initialized with zeros.
         *
         * @param size Size of the new [Complex32VectorValue]
         * @param rnd A [SplittableRandom] to generate the random numbers.
         */
        fun zero(size: Int) = DoubleVectorValue(DoubleArray(size))
    }

    constructor(input: List<Number>) : this(DoubleArray(input.size) { input[it].toDouble() })
    constructor(input: Array<Number>) : this(DoubleArray(input.size) { input[it].toDouble() })

    override val logicalSize: Int
        get() = this.data.size

    override fun compareTo(other: Value): Int {
        throw IllegalArgumentException("DoubleVectorValues can can only be compared for equality.")
    }
    /**
     * Returns the indices of this [DoubleVectorValue].
     *
     * @return The indices of this [DoubleVectorValue]
     */
    override val indices: IntRange
        get() = this.data.indices

    /**
     * Returns the i-th entry of  this [DoubleVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): DoubleValue = DoubleValue(this.data[i])

    /**
     * Returns the i-th entry of  this [DoubleVectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int) = this.data[i] != 0.0

    /**
     * Returns true, if this [DoubleVectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [DoubleVectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.data.all { it == 0.0 }

    /**
     * Returns true, if this [DoubleVectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [DoubleVectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.data.all { it == 1.0 }

    /**
     * Creates and returns a copy of this [DoubleVectorValue].
     *
     * @return Exact copy of this [DoubleVectorValue].
     */
    override fun copy(): DoubleVectorValue = DoubleVectorValue(this.data.copyOf(this.logicalSize))

    override fun plus(other: VectorValue<*>) = DoubleVectorValue(DoubleArray(this.data.size) {
        (this[it] + other[it]).asDouble().value
    })

    override fun minus(other: VectorValue<*>) = DoubleVectorValue(DoubleArray(this.data.size) {
        (this[it] - other[it]).asDouble().value
    })

    override fun times(other: VectorValue<*>) = DoubleVectorValue(DoubleArray(this.data.size) {
        (this[it] * other[it]).asDouble().value
    })

    override fun div(other: VectorValue<*>) = DoubleVectorValue(DoubleArray(this.data.size) {
        (this[it] / other[it]).asDouble().value
    })

    override fun plus(other: NumericValue<*>) = DoubleVectorValue(DoubleArray(this.logicalSize) {
        (this[it] + other.asDouble()).value
    })

    override fun minus(other: NumericValue<*>) = DoubleVectorValue(DoubleArray(this.logicalSize) {
        (this[it] - other.asDouble()).value
    })

    override fun times(other: NumericValue<*>) = DoubleVectorValue(DoubleArray(this.logicalSize) {
        (this[it] * other.asDouble()).value
    })

    override fun div(other: NumericValue<*>) = DoubleVectorValue(DoubleArray(this.logicalSize) {
        (this[it] / other.asDouble()).value
    })

    override fun pow(x: Int) = DoubleVectorValue(DoubleArray(this.data.size) {
        this[it].value.pow(x)
    })

    override fun sqrt() = DoubleVectorValue(DoubleArray(this.data.size) {
        kotlin.math.sqrt(this[it].value)
    })

    override fun abs()= DoubleVectorValue(DoubleArray(this.data.size) {
        kotlin.math.abs(this[it].value)
    })

    override fun sum(): DoubleValue = DoubleValue(this.data.sum())

    override fun distanceL1(other: VectorValue<*>): NumericValue<*> {
        var sum = 0.0
        for (i in this.indices) {
            sum += (other[i].value.toDouble() - this[i].value).absoluteValue
        }
        return DoubleValue(sum)
    }

    override fun distanceL2(other: VectorValue<*>): NumericValue<*> {
        var sum = 0.0
        for (i in this.indices) {
            sum += (other[i].value.toDouble() - this[i].value).pow(2)
        }
        return DoubleValue(kotlin.math.sqrt(sum))
    }

    override fun distanceLP(other: VectorValue<*>, p: Int): NumericValue<*> {
        var sum = 0.0
        for (i in this.indices) {
            sum += (other[i].value.toDouble() - this[i].value).pow(p)
        }
        return DoubleValue(sum.pow(1.0/p))
    }
}