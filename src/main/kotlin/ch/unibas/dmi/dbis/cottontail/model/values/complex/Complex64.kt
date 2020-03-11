package ch.unibas.dmi.dbis.cottontail.model.values.complex

import kotlin.math.sqrt

/**
 * Represents a complex number as a 2-dimensional array with double-precision 64-bit IEEE 754 floating point numbers.
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
inline class Complex64(private val complex: DoubleArray = DoubleArray(2)) {

    /**
     * Compares this value with the specified value for order.
     * Returns zero if this value is equal to the specified other value, a negative number if it's less than other,
     * or a positive number if it's greater than other.
     */
    operator fun compareTo(other: Complex64): Int {
        return (modulo() - other.modulo()).toInt()
    }

    /** Adds the other value to this value. */
    operator fun plus(other: Complex64) = Complex64(doubleArrayOf(this.complex[0] + other[0], this.complex[1] + other[1]))

    /** Subtracts the other value from this value. */
    operator fun minus(other: Complex64) = Complex64(doubleArrayOf(this.complex[0] - other[0], this.complex[1] - other[1]))

    /** Multiplies this value by the other value. */
    operator fun times(other: Complex64) = Complex64(doubleArrayOf(this.complex[0] * other[0] - this.complex[1] * other[1], this.complex[0] * other[1] + other[0] * this.complex[1]))

    /** Divides this value by the other value. */
    operator fun div(other: Complex64) = this * other.inverse()

    operator fun get(i: Int): Double {
        return this.complex[i]
    }

    private fun inverse(): Complex64 {
        return Complex64(doubleArrayOf(this.complex[0] / (this.complex[0] * this.complex[0] + this.complex[1] * this.complex[1]), -this.complex[1] / (this.complex[0] * this.complex[0] + this.complex[1] * this.complex[1])))
    }

    private fun modulo(): Double {
        return sqrt(this.complex[0] * this.complex[0] + this.complex[1] * this.complex[1])
    }
}