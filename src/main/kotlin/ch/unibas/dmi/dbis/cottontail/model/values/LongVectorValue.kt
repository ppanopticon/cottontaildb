package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.values.types.*
import java.util.*
import kotlin.math.absoluteValue
import kotlin.math.pow

/**
 * This is an abstraction over a [FloatArray] and it represents a vector of [Float]s.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
inline class LongVectorValue(val data: LongArray) : RealVectorValue<Long> {

    constructor(input: List<Number>) : this(LongArray(input.size) { input[it].toLong() })
    constructor(input: Array<Number>) : this(LongArray(input.size) { input[it].toLong() })

    override val logicalSize: Int
        get() = data.size

    override fun compareTo(other: Value): Int {
        throw IllegalArgumentException("LongVectorValues can can only be compared for equality.")
    }

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

    override fun plus(other: VectorValue<*>): LongVectorValue = LongVectorValue(LongArray(this.logicalSize) {
        (this[it] + other[it].asLong()).value
    })

    override fun minus(other: VectorValue<*>): LongVectorValue = LongVectorValue(LongArray(this.logicalSize) {
        (this[it] - other[it].asLong()).value
    })

    override fun times(other: VectorValue<*>): LongVectorValue = LongVectorValue(LongArray(this.logicalSize) {
        (this[it] * other[it].asLong()).value
    })

    override fun div(other: VectorValue<*>): LongVectorValue = LongVectorValue(LongArray(this.logicalSize) {
        (this[it] / other[it].asLong()).value
    })

    override fun plus(other: NumericValue<*>): LongVectorValue = LongVectorValue(LongArray(this.logicalSize) {
        (this[it] + other.asLong()).value
    })

    override fun minus(other: NumericValue<*>): LongVectorValue = LongVectorValue(LongArray(this.logicalSize) {
        (this[it] - other.asLong()).value
    })

    override fun times(other: NumericValue<*>): LongVectorValue = LongVectorValue(LongArray(this.logicalSize) {
        (this[it] * other.asLong()).value
    })

    override fun div(other: NumericValue<*>): LongVectorValue = LongVectorValue(LongArray(this.logicalSize) {
        (this[it] / other.asLong()).value
    })

    override fun pow(x: Int) = DoubleVectorValue(DoubleArray(this.data.size) {
        this.data[it].toDouble().pow(x)
    })

    override fun sqrt() = DoubleVectorValue(DoubleArray(this.data.size) {
        kotlin.math.sqrt(this.data[it].toDouble())
    })

    override fun abs() = LongVectorValue(LongArray(this.logicalSize) {
        kotlin.math.abs(this.data[it])
    })

    override fun sum(): LongValue = LongValue(this.data.sum())

    override fun distanceL1(other: VectorValue<*>): NumericValue<*> {
        var sum = 0L
        for (i in this.indices) {
            sum += (this.data[i] - other[i].asLong().value).absoluteValue
        }
        return DoubleValue(sum)
    }

    override fun distanceL2(other: VectorValue<*>): NumericValue<*> {
        var sum = 0.0
        for (i in this.indices) {
            sum += (this.data[i] - other[i].asInt().value).toDouble().pow(2)
        }
        return DoubleValue(kotlin.math.sqrt(sum))
    }

    override fun distanceLP(other: VectorValue<*>, p: Int): NumericValue<*> {
        var sum = 0.0
        for (i in this.indices) {
            sum += (this.data[i] - other[i].asInt().value).toDouble().pow(p)
        }
        return DoubleValue(sum.pow(1.0/p))
    }
}