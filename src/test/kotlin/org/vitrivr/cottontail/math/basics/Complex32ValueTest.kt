package org.vitrivr.cottontail.math.basics

import org.apache.commons.math3.complex.Complex
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.model.values.Complex32Value
import org.vitrivr.cottontail.model.values.FloatValue
import java.util.*

/**
 * Some basic test cases that test for correctness fo [Complex32Value] arithmetic operations.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class Complex32ValueTest {

    companion object {
        private const val DELTA = 1e-3f
        fun isCorrect(expected: Complex, actual: Complex32Value) {
            if (actual.real.value == 0.0f) {
                assertEquals(expected.real.toFloat(), actual.real.value)
            } else {
                val r_ratio = expected.real.toFloat() / actual.real.value
                Assertions.assertTrue(r_ratio > 1.0f - DELTA)
                Assertions.assertTrue(r_ratio < 1.0f + DELTA)
            }
            if (actual.imaginary.value == 0.0f) {
                assertEquals(expected.imaginary.toFloat(), actual.imaginary.value)
            } else {
                val i_ratio = expected.imaginary.toFloat() / actual.imaginary.value
                Assertions.assertTrue(i_ratio > 1.0f - DELTA)
                Assertions.assertTrue(i_ratio < 1.0f + DELTA)
            }
        }
    }

    private val random = SplittableRandom()

    @Test
    internal fun testValueCreation() {
        isCorrect(Complex(1.0, 0.0), Complex32Value(1.0f))
        isCorrect(Complex(1.0, 0.0), Complex32Value(1.0))
        isCorrect(Complex(1.0, 0.0), Complex32Value(FloatValue(1.0f)))
    }

    @RepeatedTest(25)
    fun testAdd() {
        val c1 = Complex32Value.random(random)
        val c2 = Complex32Value.random(random)
        val c264 = c2.asComplex64()

        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)
        val c2p = Complex(c2.real.asDouble().value, c2.imaginary.asDouble().value)


        val add = c1 + c2
        val add64 = c1 + c264
        val addp = c1p.add(c2p)

        isCorrect(addp, add)
        isCorrect(addp, add64)

    }

    @RepeatedTest(25)
    fun testUnaryMinus() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)
        isCorrect(c1p.multiply(-1), -c1)
    }

    @RepeatedTest(25)
    fun testMinus() {
        val c1 = Complex32Value.random(random)
        val c2 = Complex32Value.random(random)
        val c264 = c2.asComplex64()
        val r1 = FloatValue.random(random)

        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)
        val c2p = Complex(c2.real.asDouble().value, c2.imaginary.asDouble().value)

        val sub = c1 - c2
        val sub64 = c1 - c264
        val subr = c1 - r1
        val subp = c1p.subtract(c2p)
        val subpr = c1p.subtract(r1.asDouble().value)

        isCorrect(subp, sub)
        isCorrect(subp, sub64)
        isCorrect(subpr, subr)
    }

    @RepeatedTest(25)
    fun testMultiply() {
        val c1 = Complex32Value.random(random)
        val c2 = Complex32Value.random(random)
        val c264 = c2.asComplex64()

        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)
        val c2p = Complex(c2.real.asDouble().value, c2.imaginary.asDouble().value)


        val mult = c1 * c2
        val mult64 = c1 * c264
        val multp = c1p.multiply(c2p)

        isCorrect(multp, mult)
        isCorrect(multp, mult64)
    }

    @RepeatedTest(25)
    fun testMultiplyReal() {
        val c1 = Complex32Value.random(random)
        val c2 = FloatValue.random(random)
        val c2d = c2.asDouble()

        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)
        val c2p = c2.value


        val mult = c1 * c2
        val multd = c1 * c2d
        val multp = c1p.multiply(c2p.toDouble())

        isCorrect(multp, mult)
        isCorrect(multp, multd)
    }

    @RepeatedTest(25)
    fun testDivision() {
        val c1 = Complex32Value.random(random)
        val c2 = Complex32Value.random(random)
        val c264 = c2.asComplex64()

        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)
        val c2p = Complex(c2.real.asDouble().value, c2.imaginary.asDouble().value)

        val div = c1 / c2
        val div64 = c1 / c264
        val divp = c1p.divide(c2p)

        isCorrect(divp, div)
        isCorrect(divp, div64)
    }

    @RepeatedTest(25)
    fun testDivReal() {
        val c1 = Complex32Value.random(random)
        val c2 = FloatValue.random(random)
        val c2d = c2.asDouble()

        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)
        val c2p = c2.value

        val div = c1 / c2
        val divd = c1 / c2d
        val divp = c1p.divide(c2p.toDouble())

        isCorrect(divp, div)
        isCorrect(divp, divd)
    }

    @RepeatedTest(25)
    fun testInverse() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)

        val inv = c1.inverse()
        val invp = c1p.reciprocal()

        isCorrect(invp, inv)
    }

    @RepeatedTest(25)
    fun testConjugate() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)

        val conj = c1.conjugate()
        val conjp = c1p.conjugate()

        isCorrect(conjp, conj)
    }

    @RepeatedTest(25)
    fun testExp() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)

        val exp = c1.exp().asComplex32()
        val expp = c1p.exp()

        isCorrect(expp, exp)
    }

    @RepeatedTest(25)
    fun testLn() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)

        val ln = c1.ln().asComplex32()
        val lnp = c1p.log()

        isCorrect(lnp, ln)
    }


    @RepeatedTest(25)
    fun testPow() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)
        val e = random.nextDouble()

        val pow = c1.pow(e).asComplex32()
        val powp = c1p.pow(e)

        isCorrect(powp, pow)
    }

    @RepeatedTest(25)
    fun testSqrt() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)


        val sqrt = c1.sqrt().asComplex32()
        val sqrtp = c1p.sqrt()

        isCorrect(sqrtp, sqrt)
    }

    @RepeatedTest(25)
    fun testCos() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)

        val cos = c1.cos().asComplex32()
        val cosp = c1p.cos()

        isCorrect(cosp, cos)
    }

    @RepeatedTest(25)
    fun testSin() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)

        val sin = c1.sin().asComplex32()
        val sinp = c1p.sin()

        isCorrect(sinp, sin)
    }

    @RepeatedTest(25)
    fun testTan() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)

        val tan = c1.tan().asComplex32()
        val tanp = c1p.tan()

        isCorrect(tanp, tan)
    }

    @RepeatedTest(25)
    internal fun testTanLargeIm() {
        val c1 = Complex32Value(FloatValue.random(random).value, FloatValue.random(random).value + 25.0f)
        val c2 = Complex32Value(FloatValue.random(random).value, FloatValue.random(random).value - 25.0f)

        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)
        val c2p = Complex(c2.real.asDouble().value, c2.imaginary.asDouble().value)

        val tan1 = c1.tan().asComplex32()
        val tan1p = c1p.tan()

        val tan2 = c2.tan().asComplex32()
        val tan2p = c2p.tan()

        isCorrect(tan1p, tan1)
        isCorrect(tan2p, tan2)
    }

    @RepeatedTest(25)
    fun testAtan() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)

        val atan = c1.atan().asComplex32()
        val atanp = c1p.atan()

        isCorrect(atanp, atan)
    }

    @RepeatedTest(25)
    fun testAddConjugate() {
        val c1 = Complex32Value.random(random)
        val c2 = c1.conjugate()

        val add = c1 + c2

        Assertions.assertEquals(c1.real + c2.real, add.real)
        Assertions.assertEquals(0.0f, add.imaginary.value)
    }

    @RepeatedTest(25)
    fun testAbs() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)

        val abs = c1.abs()
        val absp = c1p.abs()

        isCorrect(Complex(absp), abs.asComplex32())
    }
}