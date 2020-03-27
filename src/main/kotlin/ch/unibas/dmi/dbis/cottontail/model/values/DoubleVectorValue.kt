package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.values.types.NumericValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.RealVectorValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.model.values.types.VectorValue
import java.nio.ByteBuffer
import java.util.*
import kotlin.math.absoluteValue
import kotlin.math.pow

/**
 * This is an abstraction over a [DoubleArray] and it represents a vector of [Double]s.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
inline class DoubleVectorValue(val data: ByteBuffer) : RealVectorValue<Double> {

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

    constructor(input: DoubleArray) : this(ByteBuffer.allocate(Long.SIZE_BYTES * input.size).also { b ->
        input.forEach { b.putDouble(it) }
        b.rewind()
    })
    constructor(input: List<Number>) : this(ByteBuffer.allocate(Long.SIZE_BYTES * input.size).also { b ->
        input.forEach { b.putDouble(it.toDouble()) }
        b.rewind()
    })
    constructor(input: Array<Number>) : this(ByteBuffer.allocate(Long.SIZE_BYTES * input.size).also { b ->
        input.forEach { b.putDouble(it.toDouble()) }
        b.rewind()
    })

    override val logicalSize: Int
        get() = this.data.capacity() / Long.SIZE_BYTES

    override fun compareTo(other: Value): Int {
        throw IllegalArgumentException("DoubleVectorValues can can only be compared for equality.")
    }
    /**
     * Returns the indices of this [DoubleVectorValue].
     *
     * @return The indices of this [DoubleVectorValue]
     */
    override val indices: IntRange
        get() = (0 until this.logicalSize)

    /**
     * Returns the i-th entry of  this [DoubleVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): DoubleValue = DoubleValue(this.data.getDouble(i * Long.SIZE_BYTES))

    /**
     * Returns the i-th entry of  this [DoubleVectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int) = this.data.getDouble(i * Long.SIZE_BYTES) != 0.0

    /**
     * Returns true, if this [DoubleVectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [DoubleVectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.indices.all { this.data.getDouble(it * Long.SIZE_BYTES) == 0.0 }

    /**
     * Returns true, if this [DoubleVectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [DoubleVectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.indices.all { this.data.getDouble(it * Long.SIZE_BYTES) == 1.0 }

    /**
     * Creates and returns a copy of this [DoubleVectorValue].
     *
     * @return Exact copy of this [DoubleVectorValue].
     */
    override fun copy(): DoubleVectorValue = DoubleVectorValue(ByteBuffer.allocate(this.data.capacity()).put(this.data.rewind()).rewind())

    override fun plus(other: VectorValue<*>) = DoubleVectorValue(ByteBuffer.allocate(this.data.capacity()).also { b ->
        for (i in 0 until this.logicalSize) {
            b.putDouble((this[i] + other[i]).asDouble().value)
        }
        b.rewind()
    })

    override fun minus(other: VectorValue<*>) = DoubleVectorValue(ByteBuffer.allocate(this.data.capacity()).also { b ->
        for (i in 0 until this.logicalSize) {
            b.putDouble((this[i] - other[i]).asDouble().value)
        }
        b.rewind()
    })

    override fun times(other: VectorValue<*>) = DoubleVectorValue(ByteBuffer.allocate(this.data.capacity()).also { b ->
        for (i in 0 until this.logicalSize) {
            b.putDouble((this[i] * other[i]).asDouble().value)
        }
        b.rewind()
    })

    override fun div(other: VectorValue<*>) = DoubleVectorValue(ByteBuffer.allocate(this.data.capacity()).also { b ->
        for (i in 0 until this.logicalSize) {
            b.putDouble((this[i] / other[i]).asDouble().value)
        }
        b.rewind()
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

    override fun pow(x: Int) = DoubleVectorValue(ByteBuffer.allocate(this.data.capacity()).also { b ->
        for (i in 0 until this.logicalSize) {
            this[i].value.pow(x)
        }
        b.rewind()
    })

    override fun sqrt() = DoubleVectorValue(ByteBuffer.allocate(this.data.capacity()).also { b ->
        for (i in 0 until this.logicalSize) {
            kotlin.math.sqrt(this[i].value)
        }
        b.rewind()
    })

    override fun abs() = DoubleVectorValue(ByteBuffer.allocate(this.data.capacity()).also { b ->
        for (i in 0 until this.logicalSize) {
            kotlin.math.abs(this[i].value)
        }
        b.rewind()
    })

    override fun sum(): DoubleValue = DoubleValue(this.indices.map { this[it].value }.sum())

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