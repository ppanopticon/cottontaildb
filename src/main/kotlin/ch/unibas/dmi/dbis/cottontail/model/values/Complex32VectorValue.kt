package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.values.types.*

import java.util.*

/**
 * This is an abstraction over an [Array] and it represents a vector of [Complex32]s.
 *
 * @author Manuel Huerbin & Ralph Gasser
 * @version 1.1
 */
inline class Complex32VectorValue(val data: Array<Complex32Value>) : ComplexVectorValue<Float> {


    companion object {
        /**
         * Generates a [Complex32VectorValue] of the given size initialized with random numbers.
         *
         * @param size Size of the new [Complex32VectorValue]
         * @param rnd A [SplittableRandom] to generate the random numbers.
         */
        fun random(size: Int, rnd: SplittableRandom = SplittableRandom(System.currentTimeMillis())) = Complex32VectorValue(Array(size) {
            Complex32Value(FloatValue(Float.fromBits(rnd.nextInt())), FloatValue(Float.fromBits(rnd.nextInt())))
        })

        /**
         * Generates a [Complex32VectorValue] of the given size initialized with ones.
         *
         * @param size Size of the new [Complex32VectorValue]
         */
        fun one(size: Int) = Complex32VectorValue(Array(size) {
            Complex32Value.ONE
        })

        /**
         * Generates a [Complex32VectorValue] of the given size initialized with zeros.
         *
         * @param size Size of the new [Complex32VectorValue]
         * @param rnd A [SplittableRandom] to generate the random numbers.
         */
        fun zero(size: Int) = Complex32VectorValue(Array(size) {
            Complex32Value.ZERO
        })
    }

    override val logicalSize: Int
        get() = this.data.size/2

    /**
     * Returns the i-th entry of  this [Complex32VectorValue]. All entries with i % 2 == 0 correspond
     * to the real part of the value, whereas entries with i % 2 == 1 correspond to the imaginary part.
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int) = this.data[i]
    override fun real(i: Int) = this.data[i].real
    override fun imaginary(i: Int) = this.data[i].imaginary


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
    override fun getAsBool(i: Int): Boolean = this.data[i] != Complex32Value.ZERO

    /**
     * Returns true, if this [Complex32VectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [Complex32VectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.data.all { it == Complex32Value.ZERO  }

    /**
     * Returns true, if this [Complex32VectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [Complex32VectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.data.all { it == Complex32Value.ONE }

    /**
     * Creates and returns a copy of this [Complex32VectorValue].
     *
     * @return Exact copy of this [Complex32VectorValue].
     */
    override fun copy(): Complex32VectorValue = Complex32VectorValue(data.copyOf())

    override fun plus(other: VectorValue<*>) = Complex32VectorValue(Array(this.data.size) {
        this[it] + other[it].asComplex32()
    })

    override fun minus(other: VectorValue<*>) = Complex32VectorValue(Array(this.data.size) {
        this[it] - other[it].asComplex32()
    })

    override fun times(other: VectorValue<*>) = Complex32VectorValue(Array(this.data.size) {
        this[it] * other[it].asComplex32()
    })

    override fun div(other: VectorValue<*>) = Complex32VectorValue(Array(this.data.size) {
        this[it] / other[it].asComplex32()
    })

    override fun plus(other: NumericValue<*>) = Complex32VectorValue(Array(this.data.size) {
        this[it] + other.asComplex32()
    })

    override fun minus(other: NumericValue<*>) = Complex32VectorValue(Array(this.data.size) {
        this[it] - other.asComplex32()
    })

    override fun times(other: NumericValue<*>) = Complex32VectorValue(Array(this.data.size) {
        this[it] * other.asComplex32()
    })

    override fun div(other: NumericValue<*>) = Complex32VectorValue(Array(this.data.size) {
        this[it] / other.asComplex32()
    })

    override fun pow(x: Int) = Complex64VectorValue(Array(this.data.size) {
        val a2 = this.real(it).pow(2)
        val b2 = this.imaginary(it).pow(2)
        val r = (a2 + b2).sqrt()
        val theta = (b2 / a2).atan()
        Complex64Value(r.pow(x) * ((theta * IntValue(x)).cos()), r.pow(x) * (theta * IntValue(x)).sin())
    })

    override fun sqrt() = Complex64VectorValue(Array(this.data.size) {
        Complex64Value(
            (this.real(it) + (this.real(it).pow(2) + this.imaginary(it).pow(2)).sqrt()/FloatValue(2.0f)).sqrt(),
            (this.imaginary(it) / this.imaginary(it).abs()) * (- this.real(it) + (this.real(it).pow(2) + this.imaginary(it).pow(2)).sqrt()/FloatValue(2.0f)).sqrt()
        )
    })

    override fun abs() = Complex32VectorValue(Array(this.data.size) {
        Complex32Value(this[it].real.abs(), this[it].imaginary.abs())
    })

    override fun sum(): Complex32Value {
        var real = FloatValue(0.0f)
        var imaginary = FloatValue(0.0f)
        this.indices.forEach {
            real += this.real(it)
            imaginary += this.imaginary(it)
        }
        return Complex32Value(real, imaginary)
    }

    override fun distanceL1(other: VectorValue<*>): NumericValue<*> {
        TODO("Not yet implemented")
    }

    override fun distanceL2(other: VectorValue<*>): NumericValue<*> {
        TODO("Not yet implemented")
    }

    override fun distanceLP(other: VectorValue<*>, p: Int): NumericValue<*> {
        TODO("Not yet implemented")
    }
}