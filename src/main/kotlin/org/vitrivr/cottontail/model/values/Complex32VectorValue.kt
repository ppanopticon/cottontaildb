package org.vitrivr.cottontail.model.values

import org.apache.commons.math3.util.FastMath
import org.vitrivr.cottontail.database.column.Type
import org.vitrivr.cottontail.model.values.types.ComplexVectorValue
import org.vitrivr.cottontail.model.values.types.NumericValue
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.extensions.nextFloat
import java.util.*
import kotlin.math.atan2
import kotlin.math.pow

/**
 * This is an abstraction over an [Array] and it represents a vector of [Complex32Value]s.
 *
 * @author Manuel Huerbin & Ralph Gasser
 * @version 1.5.0
 */
inline class Complex32VectorValue(val data: FloatArray) : ComplexVectorValue<Float> {
    companion object {
        /**
         * Generates a [Complex32VectorValue] of the given size initialized with random numbers.
         *
         * @param size Size of the new [Complex32VectorValue]
         * @param rnd A [SplittableRandom] to generate the random numbers.
         * @return Random [Complex32VectorValue]
         */
        fun random(size: Int, rnd: SplittableRandom = Value.RANDOM) = Complex32VectorValue(FloatArray(size * 2) { rnd.nextFloat() })

        /**
         * Generates a [Complex32VectorValue] of the given size initialized with ones (i.e 1.0f + i0.0f).
         *
         * @param size Size of the new [Complex32VectorValue]
         * @return [Complex32VectorValue] filled with ones.
         */
        fun one(size: Int) = Complex32VectorValue(FloatArray(size * 2) {
            if (it % 2 == 0) {
                1.0f
            } else {
                0.0f
            }
        })

        /**
         * Generates a [Complex32VectorValue] of the given size initialized with zeros.
         *
         * @param size Size of the new [Complex32VectorValue]
         * @return [Complex32VectorValue] filled with zeros.
         */
        fun zero(size: Int) = Complex32VectorValue(FloatArray(size * 2) { 0.0f })
    }

    /**
     * Constructor given an Array of [Complex32Value]s
     *
     * @param value Array of [Complex32Value]s
     */
    constructor(value: Array<Complex32Value>) : this(FloatArray(2 * value.size) {
        if (it % 2 == 0) {
            value[it / 2].real.value
        } else {
            value[it / 2].imaginary.value
        }
    })

    /**
     * Constructor given an Array of [Complex64Value]s
     *
     * @param value Array of [Complex32Value]s
     */
    constructor(value: Array<Complex64Value>) : this(FloatArray(2 * value.size) {
        if (it % 2 == 0) {
            value[it / 2].real.value.toFloat()
        } else {
            value[it / 2].imaginary.value.toFloat()
        }
    })

    /** Logical size of the [Complex32VectorValue]. */
    override val logicalSize: Int
        get() = this.data.size / 2

    /** The [Type] of this [Complex32VectorValue]. */
    override val type: Type<*>
        get() = Type.Complex32Vector(this.logicalSize)

    /**
     * Returns the i-th entry of  this [Complex32VectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int) = Complex32Value(this.data[i shl 1], this.data[(i shl 1) + 1])

    /**
     * Returns the real part of the i-th entry of this [Complex32VectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i as [FloatValue].
     */
    override fun real(i: Int) = FloatValue(this.data[i shl 1])

    /**
     * Returns the imaginary part of the i-th entry of this [Complex32VectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i as [FloatValue].
     */
    override fun imaginary(i: Int) = FloatValue(this.data[(i shl 1) + 1])

    /**
     * Returns a sub vector of this [DoubleVectorValue] starting at the component [start] and
     * containing [length] components.
     *
     * @param start Index of the first entry of the returned vector.
     * @param length how many elements, including start, to return
     *
     * @return The [DoubleVectorValue] representing the sub-vector.
     */
    override fun subvector(start: Int, length: Int) =
        Complex32VectorValue(this.data.copyOfRange(2 * start, 2 * start + 2 * length))


    /**
     * Checks for equality between this [Complex32VectorValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [Complex32VectorValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean =
        (other is FloatVectorValue) && (this.data.contentEquals(other.data))

    override fun compareTo(other: Value): Int {
        throw IllegalArgumentException("ComplexVectorValues can can only be compared for equality.")
    }

    /**
     * Returns the indices of this [Complex32VectorValue].
     *
     * @return The indices of this [Complex32VectorValue]
     */
    override val indices: IntRange
        get() = (0 until this.logicalSize)

    /**
     * Returns the i-th entry of  this [Complex32VectorValue] as [Boolean]. All entries with index % 2 == 0 correspond
     * to the real part of the value, whereas entries with i % 2 == 1 correspond to the imaginary part.
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int): Boolean = this[i] != Complex32Value.ZERO

    /**
     * Returns true, if this [Complex32VectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [Complex32VectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.data.all { it == 0.0f }

    /**
     * Returns true, if this [Complex32VectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [Complex32VectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.data.all { it == 1.0f }

    /**
     * Creates and returns a copy of this [Complex32VectorValue].
     *
     * @return Exact copy of this [Complex32VectorValue].
     */
    override fun copy(): Complex32VectorValue = Complex32VectorValue(this.data.copyOf())

    /**
     * Creates and returns a new instance of [Complex32VectorValue] of the same size.
     *
     * @return New instance of [Complex32VectorValue]
     */
    override fun new(): Complex32VectorValue = Complex32VectorValue(FloatArray(this.data.size))

    /**
     * Creates and returns a copy of this [ComplexVectorValue]'s real components.
     *
     * @return Copy of this [ComplexVectorValue].
     */
    override fun copyReal() = FloatVectorValue(FloatArray(this.logicalSize) { this.data[it shl 1] })

    /**
     * Creates and returns a copy of this [ComplexVectorValue]'s imaginary components.
     *
     * @return Copy of this [ComplexVectorValue].
     */
    override fun copyImaginary() =
        FloatVectorValue(FloatArray(this.logicalSize) { this.data[(it shl 1) + 1] })

    override fun plus(other: VectorValue<*>) = if (other.logicalSize == this.logicalSize)
        Complex32VectorValue(when (other) {
            is Complex32VectorValue -> FloatArray(this.data.size) { this.data[it] + other.data[it] }
            is Complex64VectorValue -> FloatArray(this.data.size) { (this.data[it] + other.data[it]).toFloat() }
            else -> FloatArray(this.data.size) {
                if (it % 2 == 0) {
                    this.data[it] + other[it / 2].value.toFloat()
                } else {
                    this.data[it]
                }
            }
        }) else throw IllegalArgumentException("Dimensions ${this.logicalSize} and ${other.logicalSize} don't agree!")

    override operator fun minus(other: VectorValue<*>) = Complex32VectorValue(
        when (other) {
            is Complex32VectorValue -> FloatArray(this.logicalSize * 2) { this.data[it] - other.data[it] }
            is Complex64VectorValue -> FloatArray(this.logicalSize * 2) { (this.data[it] - other.data[it]).toFloat() }
            else -> FloatArray(this.logicalSize * 2) {
                if (it % 2 == 0) {
                    this.data[it] - other[it / 2].value.toFloat()
                } else {
                    this.data[it]
                }
            }
        }
    )

    override fun times(other: VectorValue<*>) = if (other.logicalSize == this.logicalSize)
        Complex32VectorValue(when (other) {
            is Complex32VectorValue -> FloatArray(this.data.size) {
                if (it % 2 == 0) {
                    this.data[it] * other.data[it] - this.data[it + 1] * other.data[it + 1]
                } else {
                    this.data[it - 1] * other.data[it] + this.data[it] * other.data[it - 1]
                }
            }
            is Complex64VectorValue -> FloatArray(this.data.size) {
                if (it % 2 == 0) {
                    (this.data[it] * other.data[it] - this.data[it + 1] * other.data[it + 1]).toFloat()
                } else {
                    (this.data[it - 1] * other.data[it] + this.data[it] * other.data[it - 1]).toFloat()
                }
        }
        else -> FloatArray(this.data.size) {
            this.data[it] * other[it / 2].value.toFloat()
        }
    }) else throw IllegalArgumentException("Dimensions ${this.logicalSize} and ${other.logicalSize} don't agree!")

    override fun div(other: VectorValue<*>) = if (other.logicalSize == this.logicalSize)
        when (other) {
        is Complex64VectorValue -> internalComplex64VectorValueDiv(other)
        is Complex32VectorValue -> internalComplex32VectorValueDiv(other)
        else -> Complex32VectorValue(FloatArray(this.data.size) {
            this.data[it] / other[it / 2].value.toFloat()
        })
    } else throw IllegalArgumentException("Dimensions ${this.logicalSize} and ${other.logicalSize} don't agree!")

    /**
     * Internal division implementations for [Complex64VectorValue]s.
     */
    private fun internalComplex64VectorValueDiv(other: Complex64VectorValue): Complex32VectorValue {
        val floats = FloatArray(this.data.size)
        for (i in 0 until this.data.size / 2) {
            val c = other.data[i shl 1]
            val d = other.data[(i shl 1) + 1]
            if (kotlin.math.abs(c) < kotlin.math.abs(d)) {
                val q = c / d
                val denominator = c * q + d
                floats[i shl 1] = ((this.data[i shl 1] * q + this.data[(i shl 1) + 1]) / denominator).toFloat()
                floats[(i shl 1) + 1] = ((this.data[(i shl 1) + 1] * q - this.data[i shl 1]) / denominator).toFloat()
            } else {
                val q = d / c
                val denominator = d * q + c
                floats[i shl 1] = ((this.data[(i shl 1) + 1] * q + this.data[(i shl 1)]) / denominator).toFloat()
                floats[(i shl 1) + 1] = ((this.data[(i shl 1) + 1] - this.data[i shl 1] * q) / denominator).toFloat()
            }
        }
        return Complex32VectorValue(floats)
    }

    /**
     * Internal division implementations for [Complex32VectorValue]s.
     */
    private fun internalComplex32VectorValueDiv(other: Complex32VectorValue): Complex32VectorValue {
        val floats = FloatArray(this.data.size)
        for (i in 0 until this.data.size / 2) {
            val c = other.data[i shl 1]
            val d = other.data[(i shl 1) + 1]
            if (kotlin.math.abs(c) < kotlin.math.abs(d)) {
                val q = c / d
                val denominator = c * q + d
                floats[i shl 1] = (this.data[i shl 1] * q + this.data[(i shl 1) + 1]) / denominator
                floats[(i shl 1) + 1] = (this.data[(i shl 1) + 1] * q - this.data[i shl 1]) / denominator
            } else {
                val q = d / c
                val denominator = d * q + c
                floats[i shl 1] = (this.data[(i shl 1) + 1] * q + this.data[(i shl 1)]) / denominator
                floats[(i shl 1) + 1] = (this.data[(i shl 1) + 1] - this.data[i shl 1] * q) / denominator
            }
        }
        return Complex32VectorValue(floats)
    }

    override fun plus(other: NumericValue<*>) = Complex32VectorValue(when (other) {
        is Complex32Value -> FloatArray(this.data.size) {
            if (it % 2 == 0) {
                this.data[it] + other.data[0]
            } else {
                this.data[it] + other.data[1]
            }
        }
        is Complex64Value -> FloatArray(this.data.size) {
            if (it % 2 == 0) {
                (this.data[it] + other.data[0]).toFloat()
            } else {
                (this.data[it] + other.data[1]).toFloat()
            }
        }
        else -> FloatArray(this.data.size) {
            if (it % 2 == 0) {
                this.data[it] + other.value.toFloat()
            } else {
                this.data[it]
            }
        }
    })

    override fun minus(other: NumericValue<*>) = Complex32VectorValue(when (other) {
        is Complex32Value -> FloatArray(this.data.size) {
            if (it % 2 == 0) {
                this.data[it] - other.data[0]
            } else {
                this.data[it] - other.data[1]
            }
        }
        is Complex64Value -> FloatArray(this.data.size) {
            if (it % 2 == 0) {
                (this.data[it] - other.data[0]).toFloat()
            } else {
                (this.data[it] - other.data[1]).toFloat()
            }
        }
        else -> FloatArray(this.data.size) {
            if (it % 2 == 0) {
                this.data[it] - other.value.toFloat()
            } else {
                this.data[it]
            }
        }
    })

    override fun times(other: NumericValue<*>) = Complex32VectorValue(when (other) {
        is Complex32Value -> FloatArray(this.data.size) {
            if (it % 2 == 0) {
                this.data[it] * other.data[0] - this.data[it + 1] * other.data[1]
            } else {
                this.data[it - 1] * other.data[1] + this.data[it] * other.data[0]
            }
        }
        is Complex64Value -> FloatArray(this.data.size) {
            if (it % 2 == 0) {
                (this.data[it] * other.data[0] - this.data[it + 1] * other.data[1]).toFloat()
            } else {
                (this.data[it - 1] * other.data[1] + this.data[it] * other.data[0]).toFloat()
            }
        }
        else -> FloatArray(this.data.size) { this.data[it] * other.value.toFloat() }
    })

    override fun div(other: NumericValue<*>) = when (other) {
        is Complex64Value -> this.internalComplex64ValueDiv(other)
        is Complex32Value -> this.internalComplex32ValueDiv(other)
        else -> Complex32VectorValue(FloatArray(this.data.size) { this.data[it] / other.value.toFloat() })
    }

    /**
     * Internal division implementations for [Complex64Value]s.
     */
    private fun internalComplex64ValueDiv(other: Complex64Value): Complex32VectorValue {
        val floats = FloatArray(this.data.size)
        for (i in 0 until this.data.size / 2) {
            val c = other.data[0]
            val d = other.data[1]
            if (kotlin.math.abs(c) < kotlin.math.abs(d)) {
                val q = c / d
                val denominator = c * q + d
                floats[i shl 1] = ((this.data[i shl 1] * q + this.data[(i shl 1) + 1]) / denominator).toFloat()
                floats[(i shl 1) + 1] = ((this.data[(i shl 1) + 1] * q - this.data[i shl 1]) / denominator).toFloat()
            } else {
                val q = d / c
                val denominator = d * q + c
                floats[i shl 1] = ((this.data[(i shl 1) + 1] * q + this.data[(i shl 1)]) / denominator).toFloat()
                floats[(i shl 1) + 1] = ((this.data[(i shl 1) + 1] - this.data[i shl 1] * q) / denominator).toFloat()
            }
        }
        return Complex32VectorValue(floats)
    }

    /**
     * Internal division implementations for [Complex32Value]s.
     */
    private fun internalComplex32ValueDiv(other: Complex32Value): Complex32VectorValue {
        val floats = FloatArray(this.data.size)
        for (i in 0 until this.data.size / 2) {
            val c = other.data[0]
            val d = other.data[1]
            if (kotlin.math.abs(c) < kotlin.math.abs(d)) {
                val q = c / d
                val denominator = c * q + d
                floats[i shl 1] = (this.data[i shl 1] * q + this.data[(i shl 1) + 1]) / denominator
                floats[(i shl 1) + 1] = (this.data[(i shl 1) + 1] * q - this.data[i shl 1]) / denominator
            } else {
                val q = d / c
                val denominator = d * q + c
                floats[i shl 1] = (this.data[(i shl 1) + 1] * q + this.data[(i shl 1)]) / denominator
                floats[(i shl 1) + 1] = (this.data[(i shl 1) + 1] - this.data[i shl 1] * q) / denominator
            }
        }
        return Complex32VectorValue(floats)
    }

    override fun pow(x: Int): Complex32VectorValue {
        val floats = FloatArray(this.data.size)
        for (i in 0 until this.data.size / 2) {
            val real = x * kotlin.math.ln(kotlin.math.sqrt(this.data[i shl 1] * this.data[i shl 1] + this.data[(i shl 1) + 1] * this.data[(i shl 1) + 1]))
            val imaginary = x * atan2(this.data[(i shl 1) + 1], this.data[i shl 1])
            val exp = kotlin.math.exp(real)
            floats[i shl 1] = exp * kotlin.math.cos(imaginary)
            floats[(i shl 1) + 1] = exp * kotlin.math.sin(imaginary)
        }
        return Complex32VectorValue(floats)
    }

    /**
     * Calculates the vector of the square root values of this [Complex32VectorValue].
     *
     * @return [Complex32VectorValue] containing the square root values of this [Complex32VectorValue].
     */
    override fun sqrt(): Complex32VectorValue {
        val floats = FloatArray(this.data.size)
        for (i in 0 until this.data.size / 2) {
            if (this.data[i shl 1] == 0.0f && this.data[(i shl 1) + 1] == 0.0f) {
                continue
            }
            val modulus = kotlin.math.sqrt(this.data[i shl 1] * this.data[i shl 1] + this.data[(i shl 1) + 1] * this.data[(i shl 1) + 1])
            val t = kotlin.math.sqrt((kotlin.math.abs(this.data[i shl 1]) + modulus) / 2.0f)
            if (this.data[i shl 1] >= 0.0) {
                floats[i shl 1] = t
                floats[(i shl 1) + 1] = this.data[(i shl 1) + 1] / (2.0f * t)
            } else {
                floats[i shl 1] = kotlin.math.abs(this.data[(i shl 1) + 1]) / (2.0f * t)
                floats[(i shl 1) + 1] = FastMath.copySign(1.0f, this.data[(i shl 1) + 1]) * t
            }
        }
        return Complex32VectorValue(floats)
    }

    /**
     * Calculates the vector of the absolute values of this [Complex64VectorValue]. Note that the
     * absolute value or modulus of a complex number is a real number. Hence the resulting
     * [VectorValue] is a [DoubleVectorValue].
     *
     * @return [DoubleVectorValue] containing the absolute values of this [Complex64VectorValue].
     */
    override fun abs() = FloatVectorValue(FloatArray(this.data.size / 2) { kotlin.math.sqrt(this.data[it shl 1] * this.data[it shl 1] + this.data[(it shl 1) + 1] * this.data[(it shl 1) + 1]) })

    /**
     * Calculates the sum of the elements of this [Complex32VectorValue].
     *
     * @return Sum of this [Complex32VectorValue]'s elements.
     */
    override fun sum(): Complex32Value {
        var real = 0.0f
        var imaginary = 0.0f
        for (i in 0 until this.data.size / 2) {
            real += this.data[i shl 1]
            imaginary += this.data[(i shl 1) + 1]
        }
        return Complex32Value(real, imaginary)
    }

    override infix fun dot(other: VectorValue<*>) = when (other) {
        is Complex32VectorValue -> {
            var real = 0.0f
            var imaginary = 0.0f
            for (i in 0 until this.logicalSize) {
                val iprime = (i shl 1)
                real += this.data[iprime] * other.data[iprime] - this.data[iprime + 1] * (-other.data[iprime + 1])
                imaginary += this.data[iprime] * (-other.data[iprime + 1]) + this.data[iprime + 1] * other.data[iprime]
            }
            Complex32Value(real, imaginary)
        }
        is Complex64VectorValue -> {
            var real = 0.0f
            var imaginary = 0.0f
            for (i in 0 until this.logicalSize) {
                val iprime = (i shl 1)
                real += this.data[iprime] * other.data[iprime].toFloat() - this.data[iprime + 1] * (-other.data[iprime + 1]).toFloat()
                imaginary += this.data[iprime] * (-other.data[iprime + 1]).toFloat() + this.data[iprime + 1] * other.data[iprime].toFloat()
            }
            Complex32Value(real, imaginary)
        }
        else -> {
            var real = 0.0f
            var imaginary = 0.0f
            for (i in 0 until this.logicalSize) {
                val iprime = i shl 1
                real += this.data[iprime] * other[iprime + i].value.toFloat()
                imaginary += this.data[iprime + 1] * other[iprime + i].value.toFloat()
            }
            Complex32Value(real, imaginary)
        }
    }

    /**
     * Calculates the complex L2 norm of this [Complex64VectorValue].
     *
     * @return Complex L2 norm of this [Complex64VectorValue]
     */
    override fun norm2(): DoubleValue {
        var sum = 0.0
        for (i in 0 until this.data.size / 2) {
           sum += this.data[i shl 1] * this.data[i shl 1] + this.data[(i shl 1) + 1] * this.data[(i shl 1) + 1]
        }
        return DoubleValue(kotlin.math.sqrt(sum))
    }

    /**
     * Calculates the L1 distance between this [Complex32VectorValue] and the other [VectorValue].
     *
     * @param other The [VectorValue] to calculate the distance from.
     * @return L1 distance between this [Complex32VectorValue] and the other [VectorValue].
     */
    override fun l1(other: VectorValue<*>): DoubleValue = if (other.logicalSize == this.logicalSize)
        when (other) {
        is Complex32VectorValue -> {
            var sum = 0.0
            for (i in 0 until this.data.size / 2) {
                val diffReal = this.data[i shl 1] - other.data[i shl 1]
                val diffImaginary = this.data[(i shl 1) + 1] - other.data[(i shl 1) + 1]
                sum += kotlin.math.sqrt(diffReal.pow(2) + diffImaginary.pow(2))
            }
            DoubleValue(sum)
        }
        is Complex64VectorValue -> {
            var sum = 0.0
            for (i in 0 until this.data.size / 2) {
                val diffReal = this.data[i shl 1] - other.data[i shl 1]
                val diffImaginary = this.data[(i shl 1) + 1] - other.data[(i shl 1) + 1]
                sum += kotlin.math.sqrt(diffReal.pow(2) + diffImaginary.pow(2))
            }
            DoubleValue(sum)
        }
        else -> {
            var sum = 0.0
            for (i in 0 until this.data.size / 2) {
                val diffReal = this.data[i shl 1] - other[i].asDouble().value
                val diffImaginary = this.data[(i shl 1) + 1]
                sum += kotlin.math.sqrt(diffReal.pow(2) + diffImaginary.pow(2))
            }
            DoubleValue(sum)
        }
    } else throw IllegalArgumentException("Dimensions ${this.logicalSize} and ${other.logicalSize} don't agree!")
}