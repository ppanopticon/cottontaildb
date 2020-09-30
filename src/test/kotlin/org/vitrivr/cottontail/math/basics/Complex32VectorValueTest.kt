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
 * Some basic test cases that test for correctness of [Complex32VectorValue] arithmetic operations.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class Complex32VectorValueTest {

    private val random = SplittableRandom()

    companion object;

    @RepeatedTest(100)
    fun testAdd() {
        val size = random.nextInt(2048)

        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)
        val c264 = Complex64VectorValue(c2.data.map { it.toDouble() }.toDoubleArray())
        val c3 = DoubleVectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)
        val c3p = arrayFieldVectorFromVectorValue(c3)

        val add: Complex32VectorValue = c1 + c2
        val add64: Complex32VectorValue = c1 + c264
        val addReal: Complex32VectorValue = c1 + c3
        val addp = c1p.add(c2p)
        val addRealp = c1p.add(c3p)

        equalVectors(complex32VectorFromFieldVector(addp), add)
        equalVectors(complex32VectorFromFieldVector(addp), add64)
        equalVectors(complex32VectorFromFieldVector(addRealp), addReal)
    }

    @RepeatedTest(100)
    fun testAddScalar() {
        val size = random.nextInt(2048)
        val inc = Complex32Value.random(random)
        val real = inc.real

        val c1 = Complex32VectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)

        val add: Complex32VectorValue = c1 + inc
        val addReal: Complex32VectorValue = c1 + real
        val addp = c1p.mapAdd(Complex(inc.real.asDouble().value, inc.imaginary.asDouble().value))
        val addRealp = c1p.mapAdd(Complex(real.real.asDouble().value, real.imaginary.asDouble().value))

        equalVectors(complex32VectorFromFieldVector(addp), add)
        equalVectors(complex32VectorFromFieldVector(addRealp), addReal)
    }

    @RepeatedTest(100)
    fun testSub() {
        val size = random.nextInt(2048)

        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)
        val c264 = Complex64VectorValue(c2.data.map { it.toDouble() }.toDoubleArray())
        val c3 = DoubleVectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)
        val c3p = arrayFieldVectorFromVectorValue(c3)

        val sub: Complex32VectorValue = c1 - c2
        val sub64: Complex32VectorValue = c1 - c264
        val subReal: Complex32VectorValue = c1 - c3
        val subp = c1p.subtract(c2p)
        val subRealp = c1p.subtract(c3p)

        equalVectors(complex32VectorFromFieldVector(subp), sub)
        equalVectors(complex32VectorFromFieldVector(subp), sub64)
        equalVectors(complex32VectorFromFieldVector(subRealp), subReal)
    }

    @RepeatedTest(100)
    fun testSubScalar() {
        val size = random.nextInt(2048)
        val number = Complex32Value.random(random)
        val real = number.real

        val c1 = Complex32VectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)

        val sub: Complex32VectorValue = c1 - number
        val subReal: Complex32VectorValue = c1 - real
        val subp = c1p.mapSubtract(Complex(number.real.asDouble().value, number.imaginary.asDouble().value))
        val subRealp = c1p.mapSubtract(Complex(real.real.asDouble().value, real.imaginary.asDouble().value))

        equalVectors(complex32VectorFromFieldVector(subp), sub)
        equalVectors(complex32VectorFromFieldVector(subRealp), subReal)
    }

    @RepeatedTest(100)
    fun testMult() {
        val size = random.nextInt(2048)

        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)
        val c264 = Complex64VectorValue(c2.data.map { it.toDouble() }.toDoubleArray())
        val c3 = DoubleVectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)
        val c3p = arrayFieldVectorFromVectorValue(c3)

        val mul: Complex32VectorValue = c1 * c2
        val mul64: Complex32VectorValue = c1 * c264
        val mulReal: Complex32VectorValue = c1 * c3
        val mulp = c1p.ebeMultiply(c2p)
        val mulRealp = c1p.ebeMultiply(c3p)

        equalVectors(complex32VectorFromFieldVector(mulp), mul)
        equalVectors(complex32VectorFromFieldVector(mulp), mul64)
        equalVectors(complex32VectorFromFieldVector(mulRealp), mulReal)
    }

    @RepeatedTest(100)
    fun testMultScalar() {
        val size = random.nextInt(2048)
        val fac = Complex32Value.random(random)
        val real = fac.real

        val c1 = Complex32VectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)

        val mult: Complex32VectorValue = c1 * fac
        val multReal: Complex32VectorValue = c1 * real
        val multp = c1p.mapMultiply(Complex(fac.real.asDouble().value, fac.imaginary.asDouble().value))
        val multRealp = c1p.mapMultiply(Complex(real.real.asDouble().value, real.imaginary.asDouble().value))

        equalVectors(complex32VectorFromFieldVector(multp), mult)
        equalVectors(complex32VectorFromFieldVector(multRealp), multReal)
    }

    @RepeatedTest(100)
    fun testDiv() {
        val size = random.nextInt(2048)

        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)
        val c264 = Complex64VectorValue(c2.data.map { it.toDouble() }.toDoubleArray())
        val c3 = DoubleVectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)
        val c3p = arrayFieldVectorFromVectorValue(c3)

        val div: Complex32VectorValue = c1 / c2
        val div64: Complex32VectorValue = c1 / c264
        val divReal: Complex32VectorValue = c1 / c3
        val divp = c1p.ebeDivide(c2p)
        val divRealp = c1p.ebeDivide(c3p)

        equalVectors(complex32VectorFromFieldVector(divp), div)
        equalVectors(complex32VectorFromFieldVector(divp), div64)
        equalVectors(complex32VectorFromFieldVector(divRealp), divReal)
    }

    @RepeatedTest(100)
    fun testDivScalar() {
        val size = random.nextInt(2048)
        val number = Complex32Value.random(random)
        val real = number.real

        val c1 = Complex32VectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)

        val div: Complex32VectorValue = c1 / number
        val divReal: Complex32VectorValue = c1 / real
        val divp = c1p.mapDivide(Complex(number.real.asDouble().value, number.imaginary.asDouble().value))
        val divRealp = c1p.mapDivide(Complex(real.real.asDouble().value, real.imaginary.asDouble().value))

        equalVectors(complex32VectorFromFieldVector(divp), div)
        equalVectors(complex32VectorFromFieldVector(divRealp), divReal)
    }

    @RepeatedTest(100)
    fun testPow() {
        val size = random.nextInt(2048)
        val exp = random.nextInt(10)
        val c1 = Complex32VectorValue.random(size, this.random)

        val pow: Complex32VectorValue = c1.pow(exp)

        for (i in 0 until size) {
            val powp = c1[i].pow(exp)
            isApproximatelyTheSame(powp.real.value.toFloat(), pow[i].real.value)
            isApproximatelyTheSame(powp.imaginary.value.toFloat(), pow[i].imaginary.value)
        }
    }

    @RepeatedTest(100)
    fun testAbs() {
        val size = random.nextInt(2048)
        val c1 = Complex32VectorValue.random(size, this.random)
        val abs: FloatVectorValue = c1.abs()

        for (i in 0 until size) {
            val absp = c1[i].abs()
            isApproximatelyTheSame(absp.value, abs[i].value)
        }
    }

    @RepeatedTest(100)
    fun testSqrt() {
        val size = random.nextInt(2048)
        val c1 = Complex32VectorValue.random(size, this.random)

        val sqrt: Complex32VectorValue = c1.sqrt()

        for (i in 0 until size) {
            val sqrtp = c1[i].sqrt()
            isApproximatelyTheSame(sqrtp.real.value.toFloat(), sqrt[i].real.value)
            isApproximatelyTheSame(sqrtp.imaginary.value.toFloat(), sqrt[i].imaginary.value)
        }
    }

    @RepeatedTest(100)
    fun testDot() {
        val size = random.nextInt(2048)

        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)
        val c264 = Complex64VectorValue(c2.data.map { it.toDouble() }.toDoubleArray())
        val r1 = DoubleVectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)
        val r1p = arrayFieldVectorFromVectorValue(r1)

        val dot: Complex32Value = c1.dot(c2)
        val dot64: Complex32Value = c1.dot(c264)
        val dotReal: Complex32Value = c1.dot(r1)
        val dotReversed: Complex32Value = c2.dot(c1)

        isApproximatelyTheSame(dot.real.value, dotReversed.conjugate().real.value)
        isApproximatelyTheSame(dot.imaginary.value, dotReversed.conjugate().imaginary.value)

        var dotp = Complex32Value(0.0, 0.0)
        for (i in 0 until size) {
            dotp += c1[i] * c2[i].conjugate()
        }

        isApproximatelyTheSame(dotp.real.value, dot.real.value)
        isApproximatelyTheSame(dotp.imaginary.value, dot.imaginary.value)

        val dotp2 = c1p.dotProduct(conjFromFromComplexFieldVector(c2p))
        val dotRealp2 = c1p.dotProduct(conjFromFromComplexFieldVector(r1p))

        isApproximatelyTheSame(dotp2.real.toFloat(), dot.real.value)
        isApproximatelyTheSame(dotp2.imaginary.toFloat(), dot.imaginary.value)
        isApproximatelyTheSame(dotp2.real.toFloat(), dot64.real.value)
        isApproximatelyTheSame(dotp2.imaginary.toFloat(), dot64.imaginary.value)
        isApproximatelyTheSame(dotRealp2.real.toFloat(), dotReal.real.value)
        isApproximatelyTheSame(dotRealp2.imaginary.toFloat(), dotReal.imaginary.value)
    }

    @RepeatedTest(100)
    fun testSum() {
        val size = random.nextInt(2048)
        val c1 = Complex32VectorValue.random(size, this.random)
        val sum: Complex32Value = c1.sum()

        var sump = Complex32Value(0.0f, 0.0f)
        for (i in 0 until size) {
            sump += c1[i]
        }

        isApproximatelyTheSame(sump.real.value, sum.real.value)
        isApproximatelyTheSame(sump.imaginary.value, sum.imaginary.value)
    }

    @RepeatedTest(100)
    fun testL1() {
        val size = random.nextInt(2048)
        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)

        val l1: DoubleValue = c1.l1(c2)
        val l1p = (c1 - c2).abs().sum()
        val l2p2 = absFromFromComplexFieldVector(c1p.subtract(c2p)).l1Norm

        isApproximatelyTheSame(l1p.real.value, l1.real.value.toFloat())
        isApproximatelyTheSame(l1p.imaginary.value, l1.imaginary.value.toFloat())
        isApproximatelyTheSame(l2p2.toFloat(), l1.asFloat().value)
    }

    @RepeatedTest(100)
    fun testL2() {
        val size = random.nextInt(2048)
        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)

        val l2: DoubleValue = c1.l2(c2).asDouble()
        val l2p = (c1 - c2).abs().pow(2).sum().pow(0.5).asComplex32()
        val l2p2 = absFromFromComplexFieldVector(c1p.subtract(c2p)).norm

        isApproximatelyTheSame(l2p.real.value, l2.real.value.toFloat())
        isApproximatelyTheSame(l2p.imaginary.value, l2.imaginary.value.toFloat())
        isApproximatelyTheSame(l2p2.toFloat(), l2.asFloat().value)
    }

    @RepeatedTest(100)
    fun testLp() {
        val size = random.nextInt(2048)
        val p = random.nextInt(2, 10)
        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)

        val lp: DoubleValue = c1.lp(c2, p) as DoubleValue
        val lpp = (c1 - c2).abs().pow(p).sum().pow(1.0 / p)

        isApproximatelyTheSame(lpp.real.value.toFloat(), lp.real.value.toFloat())
        isApproximatelyTheSame(0.0f, lp.imaginary.value.toFloat())
    }

    @Test
    fun testNorm2() {
        val v = Complex32VectorValue(floatArrayOf(.0f, -3.0f, 4.2f, 3.4f, -2.1f, 0.0f))
        isApproximatelyTheSame(6.527633568147036f, v.norm2().asFloat().value)
        val o = Complex32VectorValue(floatArrayOf(.0f, .0f, .0f, .0f, .0f, .0f))
        isApproximatelyTheSame(0.0f, o.norm2().asFloat().value)
    }

    @Test
    fun testSizeMismatchFails() {
        val size = random.nextInt(2048)
        val c1 = Complex32VectorValue.random(size, random)
        val c2 = Complex32VectorValue.random(size + 1, random)

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
