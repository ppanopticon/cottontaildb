package org.vitrivr.cottontail.math.basics

import org.apache.commons.math3.complex.Complex
import org.apache.commons.math3.exception.DimensionMismatchException
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.vitrivr.cottontail.math.*
import org.vitrivr.cottontail.model.values.*
import java.util.*

/**
 * Some basic test cases that test for correctness of [Complex64VectorValue] arithmetic operations.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class Complex64VectorValueTest {

    private val random = SplittableRandom()

    companion object;

    @RepeatedTest(100)
    fun testAdd() {
        val size = random.nextInt(2048)

        val c1 = Complex64VectorValue.random(size, this.random)
        val c2 = Complex64VectorValue.random(size, this.random)
        val c232 = Complex32VectorValue(c2.data.map { it.toFloat() }.toFloatArray())
        val c3 = DoubleVectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)
        val c3p = arrayFieldVectorFromVectorValue(c3)

        val add: Complex64VectorValue = c1 + c2
        val add32 : Complex64VectorValue = c1 + c232
        val addReal: Complex64VectorValue = c1 + c3
        val addp = c1p.add(c2p)
        val addRealp = c1p.add(c3p)

        equalVectors(complex64VectorFromFieldVector(addp), add)
        equalVectors(complex32VectorFromFieldVector(addp), add32)
        equalVectors(complex64VectorFromFieldVector(addRealp), addReal)
    }

    @RepeatedTest(100)
    fun testAddScalar() {
        val size = random.nextInt(2048)
        val inc = Complex64Value.random(random)
        val real = inc.real

        val c1 = Complex64VectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)

        val add: Complex64VectorValue = c1 + inc
        val addReal: Complex64VectorValue = c1 + real
        val addp = c1p.mapAdd(Complex(inc.real.value, inc.imaginary.value))
        val addRealp = c1p.mapAdd(Complex(real.real.value, real.imaginary.value))

        equalVectors(complex64VectorFromFieldVector(addp), add)
        equalVectors(complex64VectorFromFieldVector(addRealp), addReal)
    }

    @RepeatedTest(100)
    fun testSub() {
        val size = random.nextInt(2048)

        val c1 = Complex64VectorValue.random(size, this.random)
        val c2 = Complex64VectorValue.random(size, this.random)
        val c232 = Complex32VectorValue(c2.data.map { it.toFloat() }.toFloatArray())
        val c3 = DoubleVectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)
        val c3p = arrayFieldVectorFromVectorValue(c3)

        val sub: Complex64VectorValue = c1 - c2
        val sub32 : Complex64VectorValue = c1 - c232
        val subReal: Complex64VectorValue = c1 - c3
        val subp = c1p.subtract(c2p)
        val subRealp = c1p.subtract(c3p)

        equalVectors(complex64VectorFromFieldVector(subp), sub)
        equalVectors(complex32VectorFromFieldVector(subp), sub32)
        equalVectors(complex64VectorFromFieldVector(subRealp), subReal)
    }

    @RepeatedTest(100)
    fun testSubScalar() {
        val size = random.nextInt(2048)
        val number = Complex64Value.random(random)
        val real = number.real

        val c1 = Complex64VectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)

        val sub: Complex64VectorValue = c1 - number
        val subReal: Complex64VectorValue = c1 - real
        val subp = c1p.mapSubtract(Complex(number.real.value, number.imaginary.value))
        val subRealp = c1p.mapSubtract(Complex(real.real.value, real.imaginary.value))

        equalVectors(complex64VectorFromFieldVector(subp), sub)
        equalVectors(complex64VectorFromFieldVector(subRealp), subReal)
    }

    @RepeatedTest(100)
    fun testMult() {
        val size = random.nextInt(2048)

        val c1 = Complex64VectorValue.random(size, this.random)
        val c2 = Complex64VectorValue.random(size, this.random)
        val c232 = Complex32VectorValue(c2.data.map { it.toFloat() }.toFloatArray())
        val c3 = DoubleVectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)
        val c3p = arrayFieldVectorFromVectorValue(c3)

        val mul: Complex64VectorValue = c1 * c2
        val mul32 : Complex64VectorValue = c1 * c232
        val mulReal: Complex64VectorValue = c1 * c3
        val mulp = c1p.ebeMultiply(c2p)
        val mulRealp = c1p.ebeMultiply(c3p)

        equalVectors(complex64VectorFromFieldVector(mulp), mul)
        equalVectors(complex32VectorFromFieldVector(mulp), mul32)
        equalVectors(complex64VectorFromFieldVector(mulRealp), mulReal)
    }

    @RepeatedTest(100)
    fun testMultScalar() {
        val size = random.nextInt(2048)
        val fac = Complex64Value.random(random)
        val real = fac.real

        val c1 = Complex64VectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)

        val mult: Complex64VectorValue = c1 * fac
        val multReal: Complex64VectorValue = c1 * real
        val multp = c1p.mapMultiply(Complex(fac.real.value, fac.imaginary.value))
        val multRealp = c1p.mapMultiply(Complex(real.real.value, real.imaginary.value))

        equalVectors(complex64VectorFromFieldVector(multp), mult)
        equalVectors(complex64VectorFromFieldVector(multRealp), multReal)
    }

    @RepeatedTest(100)
    fun testDiv() {
        val size = random.nextInt(2048)

        val c1 = Complex64VectorValue.random(size, this.random)
        val c2 = Complex64VectorValue.random(size, this.random)
        val c232 = Complex32VectorValue(c2.data.map { it.toFloat() }.toFloatArray())
        val c3 = DoubleVectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)
        val c3p = arrayFieldVectorFromVectorValue(c3)

        val div: Complex64VectorValue = c1 / c2
        val div32 : Complex64VectorValue = c1 / c232
        val divReal: Complex64VectorValue = c1 / c3
        val divp = c1p.ebeDivide(c2p)
        val divRealp = c1p.ebeDivide(c3p)

        equalVectors(complex64VectorFromFieldVector(divp), div)
        equalVectors(complex32VectorFromFieldVector(divp), div32)
        equalVectors(complex64VectorFromFieldVector(divRealp), divReal)
    }

    @RepeatedTest(100)
    fun testDivScalar() {
        val size = random.nextInt(2048)
        val number = Complex64Value.random(random)
        val real = number.real

        val c1 = Complex64VectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)

        val div: Complex64VectorValue = c1 / number
        val divReal: Complex64VectorValue = c1 / real
        val divp = c1p.mapDivide(Complex(number.real.value, number.imaginary.value))
        val divRealp = c1p.mapDivide(Complex(real.real.value, real.imaginary.value))

        equalVectors(complex64VectorFromFieldVector(divp), div)
        equalVectors(complex64VectorFromFieldVector(divRealp), divReal)
    }

    @RepeatedTest(100)
    fun testPow() {
        val size = random.nextInt(2048)
        val exp = random.nextInt(10)
        val c1 = Complex64VectorValue.random(size, this.random)

        val pow: Complex64VectorValue = c1.pow(exp)

        for (i in 0 until size) {
            val powp = c1[i].pow(exp)
            isApproximatelyTheSame(powp.real.value, pow[i].real.value)
            isApproximatelyTheSame(powp.imaginary.value, pow[i].imaginary.value)
        }
    }

    @RepeatedTest(100)
    fun testAbs() {
        val size = random.nextInt(2048)
        val c1 = Complex64VectorValue.random(size, this.random)
        val abs: DoubleVectorValue = c1.abs()

        for (i in 0 until size) {
            val absp = c1[i].abs()
            isApproximatelyTheSame(absp.value, abs[i].value)
        }
    }

    @RepeatedTest(100)
    fun testSqrt() {
        val size = random.nextInt(2048)
        val c1 = Complex64VectorValue.random(size, this.random)

        val sqrt: Complex64VectorValue = c1.sqrt()

        for (i in 0 until size) {
            val sqrtp = c1[i].sqrt()
            isApproximatelyTheSame(sqrtp.real.value, sqrt[i].real.value)
            isApproximatelyTheSame(sqrtp.imaginary.value, sqrt[i].imaginary.value)
        }
    }

    @RepeatedTest(100)
    fun testDot() {
        val size = random.nextInt(2048)

        val c1 = Complex64VectorValue.random(size, this.random)
        val c2 = Complex64VectorValue.random(size, this.random)
        val c232 = Complex32VectorValue(c2.data.map { it.toFloat() }.toFloatArray())
        val r1 = DoubleVectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)
        val r1p = arrayFieldVectorFromVectorValue(r1)

        val dot: Complex64Value = c1.dot(c2)
        val dotRealPart: DoubleValue = c1.dotRealPart(c2)
        val dot32 : Complex64Value = c1.dot(c232)
        val dotReal: Complex64Value = c1.dot(r1)
        val dotReversed: Complex64Value = c2.dot(c1)

        isApproximatelyTheSame(dot.real.value, dotReversed.conjugate().real.value)
        isApproximatelyTheSame(dot.imaginary.value, dotReversed.conjugate().imaginary.value)

        var dotp = Complex64Value(0.0, 0.0)
        for (i in 0 until size) {
            dotp += c1[i] * c2[i].conjugate()
        }

        isApproximatelyTheSame(dotp.real.value, dot.real.value)
        isApproximatelyTheSame(dotp.imaginary.value, dot.imaginary.value)

        val dotp2 = c1p.dotProduct(conjFromFromComplexFieldVector(c2p))
        val dotRealp2 = c1p.dotProduct(conjFromFromComplexFieldVector(r1p))

        isApproximatelyTheSame(dotp2.real, dot.real.value)
        isApproximatelyTheSame(dotp2.imaginary, dot.imaginary.value)
        isApproximatelyTheSame(dotp.real.value, dotRealPart.value)
        isApproximatelyTheSame(dotp2.real.toFloat(), dot32.real.value)
        isApproximatelyTheSame(dotp2.imaginary.toFloat(), dot32.imaginary.value)
        isApproximatelyTheSame(dotRealp2.real, dotReal.real.value)
        isApproximatelyTheSame(dotRealp2.imaginary, dotReal.imaginary.value)
    }

    @RepeatedTest(100)
    fun testSum() {
        val size = random.nextInt(2048)
        val c1 = Complex64VectorValue.random(size, this.random)
        val sum: Complex64Value = c1.sum()

        var sump = Complex64Value(0.0, 0.0)
        for (i in 0 until size) {
            sump += c1[i]
        }

        isApproximatelyTheSame(sump.real.value, sum.real.value)
        isApproximatelyTheSame(sump.imaginary.value, sum.imaginary.value)
    }

    @RepeatedTest(100)
    fun testL1() {
        val size = random.nextInt(2048)
        val c1 = Complex64VectorValue.random(size, this.random)
        val c2 = Complex64VectorValue.random(size, this.random)
        val c232 = Complex32VectorValue(c2.data.map { it.toFloat() }.toFloatArray())

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)

        val l1: DoubleValue = c1.l1(c2)
        val l132 : DoubleValue = c1.l1(c232)
        val l1p = (c1 - c2).abs().sum()
        val l1p2 = absFromFromComplexFieldVector(c1p.subtract(c2p)).l1Norm

        isApproximatelyTheSame(l1p.real.value, l1.real.value)
        isApproximatelyTheSame(l1p.imaginary.value, l1.imaginary.value)
        isApproximatelyTheSame(l1p2, l1.value)
        isApproximatelyTheSame(l1p2.toFloat(), l132.value)
    }

    @RepeatedTest(100)
    fun testL2() {
        val size = random.nextInt(2048)
        val c1 = Complex64VectorValue.random(size, this.random)
        val c2 = Complex64VectorValue.random(size, this.random)
        val c232 = Complex32VectorValue(c2.data.map { it.toFloat() }.toFloatArray())

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)

        val l2: DoubleValue = c1.l2(c2).asDouble()
        val l232 : DoubleValue = c1.l2(c232).asDouble()
        val l2p = (c1 - c2).abs().pow(2).sum().pow(0.5).asComplex64()
        val l2p2 = absFromFromComplexFieldVector(c1p.subtract(c2p)).norm

        isApproximatelyTheSame(l2p.real.value, l2.real.value)
        isApproximatelyTheSame(l2p.imaginary.value, l2.imaginary.value)
        isApproximatelyTheSame(l2p2, l2.value)
        isApproximatelyTheSame(l2p2.toFloat(), l232.value)
    }

    @RepeatedTest(100)
    fun testLp() {
        val size = random.nextInt(2048)
        val p = random.nextInt(2, 10)
        val c1 = Complex64VectorValue.random(size, this.random)
        val c2 = Complex64VectorValue.random(size, this.random)
        val c232 = Complex32VectorValue(c2.data.map { it.toFloat() }.toFloatArray())

        val lp: DoubleValue = c1.lp(c2, p).asDouble()
        val lp32 : DoubleValue = c1.lp(c232, p).asDouble()
        val lpp = (c1 - c2).abs().pow(p).sum().pow(1.0 / p)

        isApproximatelyTheSame(lpp.real.value, lp.real.value)
        isApproximatelyTheSame(0.0, lp.imaginary.value)
        isApproximatelyTheSame(lpp.real.value.toFloat(), lp32.real.value)
        isApproximatelyTheSame(0.0, lp32.imaginary.value)
    }

    @RepeatedTest(100)
    fun testRealLp() {
        val size = random.nextInt(2048)
        val p = random.nextInt(2, 10)
        val c1p = DoubleVectorValue.random(size, this.random)
        val c2p = DoubleVectorValue.random(size, this.random)

        val c1 = Complex64VectorValue(DoubleArray(2 * size) {
            if (it % 2 == 0) {
                c1p.data[it / 2]
            } else {
                0.0
            }
        })
        val c2 = Complex64VectorValue(DoubleArray(2 * size) {
            if (it % 2 == 0) {
                c2p.data[it / 2]
            } else {
                0.0
            }
        })

        val lp: DoubleValue = c1.lp(c2, p) as DoubleValue
        val lpp = c1p.lp(c2p, p)

        isApproximatelyTheSame(lpp.real.value, lp.real.value)
        isApproximatelyTheSame(0.0, lp.imaginary.value)
    }

    @Test
    fun testNorm2() {
        val v = Complex64VectorValue(doubleArrayOf(.0, -3.0, 4.2, 3.4, -2.1, 0.0))
        isApproximatelyTheSame(6.527633568147036, v.norm2().value)
        val o = Complex64VectorValue(doubleArrayOf(.0, .0, .0, .0, .0, .0))
        isApproximatelyTheSame(0.0, o.norm2().value)
    }

    @Test
    fun testSizeMismatchFails() {
        val size = random.nextInt(2048)
        val c1 = Complex64VectorValue.random(size, random)
        val c2 = Complex64VectorValue.random(size + 1, random)

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)

        assertThrows<DimensionMismatchException> { c1p.add(c2p) }
        assertThrows<IllegalArgumentException> { c1 + c2 }
        assertThrows<IllegalArgumentException> { c2 + c1 }
        assertThrows<DimensionMismatchException> { c1p.subtract(c2p) }
        assertThrows<IllegalArgumentException> { c1 - c2 }
        assertThrows<IllegalArgumentException> { c2 - c1 }
        assertThrows<DimensionMismatchException> { c1p.ebeMultiply(c2p) }
        assertThrows<IllegalArgumentException> { c1 * c2 }
        assertThrows<IllegalArgumentException> { c2 * c1 }
        assertThrows<DimensionMismatchException> { c1p.ebeDivide(c2p) }
        assertThrows<IllegalArgumentException> { c1 / c2 }
        assertThrows<IllegalArgumentException> { c2 / c1 }
        assertThrows<DimensionMismatchException> { c1p.dotProduct(c2p) }
        assertThrows<IllegalArgumentException> { c1 dot c2 }
        assertThrows<IllegalArgumentException> { c2 dot c1 }

    }
}
