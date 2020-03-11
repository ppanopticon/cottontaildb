package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.values.types.*
import java.util.*
import kotlin.math.absoluteValue
import kotlin.math.pow

/**
 * This is an abstraction over an [IntArray] and it represents a vector of [Int]s.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
inline class IntVectorValue(val data: IntArray) : RealVectorValue<Int> {

    constructor(input: List<Number>) : this(IntArray(input.size) { input[it].toInt() })
    constructor(input: Array<Number>) : this(IntArray(input.size) { input[it].toInt() })

    override val logicalSize: Int
        get() = this.data.size

    override fun compareTo(other: Value): Int {
        throw IllegalArgumentException("IntVectorValues can can only be compared for equality.")
    }

    /**
     * Returns the indices of this [IntVectorValue].
     *
     * @return The indices of this [IntVectorValue]
     */
    override val indices: IntRange
        get() = this.data.indices

    /**
     * Returns the i-th entry of  this [IntVectorValue].
     *
     * @param i Index of the entry.
     */
    override fun get(i: Int) = IntValue(this.data[i])

    /**
     * Returns the i-th entry of  this [IntVectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int) = this.data[i] != 0

    /**
     * Returns true, if this [IntVectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [IntVectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.data.all { it == 0 }

    /**
     * Returns true, if this [IntVectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [IntVectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.data.all { it == 1 }

    /**
     * Creates and returns a copy of this [IntVectorValue].
     *
     * @return Exact copy of this [IntVectorValue].
     */
    override fun copy(): IntVectorValue = IntVectorValue(this.data.copyOf(this.logicalSize))

    override fun plus(other: VectorValue<*>) = IntVectorValue(IntArray(this.logicalSize) {
        (this[it] + other[it].asInt()).value
    })

    override fun minus(other: VectorValue<*>) = IntVectorValue(IntArray(this.logicalSize) {
        (this[it] - other[it].asInt()).value
    })

    override fun times(other: VectorValue<*>) = IntVectorValue(IntArray(this.logicalSize) {
        (this[it] * other[it].asInt()).value
    })

    override fun div(other: VectorValue<*>) = IntVectorValue(IntArray(this.logicalSize) {
        (this[it] / other[it].asInt()).value
    })

    override fun plus(other: NumericValue<*>) = IntVectorValue(IntArray(this.logicalSize) {
        (this[it] + other.asInt()).value
    })

    override fun minus(other: NumericValue<*>) = IntVectorValue(IntArray(this.logicalSize) {
        (this[it] - other.asInt()).value
    })

    override fun times(other: NumericValue<*>) = IntVectorValue(IntArray(this.logicalSize) {
        (this[it] * other.asInt()).value
    })

    override fun div(other: NumericValue<*>) = IntVectorValue(IntArray(this.logicalSize) {
        (this[it] / other.asInt()).value
    })

    override fun pow(x: Int) = DoubleVectorValue(DoubleArray(this.data.size) {
        this.data[it].toDouble().pow(x)
    })

    override fun sqrt() = DoubleVectorValue(DoubleArray(this.data.size) {
        kotlin.math.sqrt(this.data[it].toDouble())
    })

    override fun abs() = IntVectorValue(IntArray(this.data.size) {
        kotlin.math.abs(this.data[it])
    })

    override fun sum(): IntValue = IntValue(this.data.sum())

    override fun distanceL1(other: VectorValue<*>): NumericValue<*> {
        var sum = 0
        for (i in this.indices) {
            sum += (this.data[i] - other[i].asInt().value).absoluteValue
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