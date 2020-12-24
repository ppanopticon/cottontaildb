package org.vitrivr.cottontail.math.basics

import org.apache.commons.math3.linear.ArrayRealVector
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.math.isApproximatelyTheSame
import org.vitrivr.cottontail.model.values.FloatVectorValue
import java.util.*
import kotlin.math.pow

/**
 * Some basic test cases that test for correctness fot [FloatVectorValue] arithmetic operations.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FloatVectorValueTest {

    private val random = SplittableRandom()

    @RepeatedTest(25)
    fun testAdd() {
        val size = random.nextInt(2048)

        val c1 = FloatVectorValue.random(size, this.random)
        val c2 = FloatVectorValue.random(size, this.random)

        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it].toDouble() })
        val c2p = ArrayRealVector(DoubleArray(c2.data.size) { c2.data[it].toDouble() })

        val add = c1 + c2
        val addp = c1p.add(c2p)

        for (i in 0 until size) {
            isApproximatelyTheSame(addp.getEntry(i).toFloat(), add[i].value)
        }
    }

    @RepeatedTest(25)
    fun testSub() {
        val size = random.nextInt(2048)

        val c1 = FloatVectorValue.random(size, this.random)
        val c2 = FloatVectorValue.random(size, this.random)

        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it].toDouble() })
        val c2p = ArrayRealVector(DoubleArray(c2.data.size) { c2.data[it].toDouble() })

        val sub = c1 - c2
        val subp = c1p.subtract(c2p)

        for (i in 0 until size) {
            isApproximatelyTheSame(subp.getEntry(i).toFloat(), sub[i].value)
        }
    }

    @RepeatedTest(25)
    fun testMul() {
        val size = random.nextInt(2048)

        val c1 = FloatVectorValue.random(size, this.random)
        val c2 = FloatVectorValue.random(size, this.random)

        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it].toDouble() })
        val c2p = ArrayRealVector(DoubleArray(c2.data.size) { c2.data[it].toDouble() })

        val mul = c1 * c2
        val mulp = c1p.ebeMultiply(c2p)

        for (i in 0 until size) {
            isApproximatelyTheSame(mulp.getEntry(i).toFloat(), mul[i].value)
        }
    }

    @RepeatedTest(25)
    fun testDiv() {
        val size = random.nextInt(2048)

        val c1 = FloatVectorValue.random(size, this.random)
        val c2 = FloatVectorValue.random(size, this.random)

        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it].toDouble() })
        val c2p = ArrayRealVector(DoubleArray(c2.data.size) { c2.data[it].toDouble() })

        val div = c1 / c2
        val divp = c1p.ebeDivide(c2p)

        for (i in 0 until size) {
            isApproximatelyTheSame(divp.getEntry(i).toFloat(), div[i].value)
        }
    }

    @RepeatedTest(25)
    fun testPow() {
        val size = random.nextInt(2048)
        val exp = random.nextInt(10)

        val c1 = FloatVectorValue.random(size, this.random)
        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it].toDouble() })

        val pow = c1.pow(exp)
        val powp = c1p.map { it.pow(exp) }

        for (i in 0 until size) {
            isApproximatelyTheSame(powp.getEntry(i).toFloat(), pow[i].value.toFloat())
        }
    }

    @RepeatedTest(25)
    fun testSqrt() {
        val size = random.nextInt(2048)

        val c1 = FloatVectorValue.random(size, this.random)
        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it].toDouble() })

        val sqrt = c1.sqrt()
        val sqrtp = c1p.map { kotlin.math.sqrt(it) }

        for (i in 0 until size) {
            isApproximatelyTheSame(sqrtp.getEntry(i).toFloat(), sqrt[i].value.toFloat())
        }
    }

    @RepeatedTest(25)
    fun testL1() {
        val size = random.nextInt(2048)

        val c1 = FloatVectorValue.random(size, this.random)
        val c2 = FloatVectorValue.random(size, this.random)

        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it].toDouble() })
        val c2p = ArrayRealVector(DoubleArray(c2.data.size) { c2.data[it].toDouble() })

        val l1 = c1.l1(c2)
        val l1p = c1p.getL1Distance(c2p)

        isApproximatelyTheSame(l1p.toFloat(), l1.value.toFloat())
    }

    @RepeatedTest(25)
    fun testL2() {
        val size = random.nextInt(2048)

        val c1 = FloatVectorValue.random(size, this.random)
        val c2 = FloatVectorValue.random(size, this.random)

        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it].toDouble() })
        val c2p = ArrayRealVector(DoubleArray(c2.data.size) { c2.data[it].toDouble() })

        val l2 = c1.l2(c2)
        val l2p = c1p.getDistance(c2p)

        isApproximatelyTheSame(l2p.toFloat(), l2.value.toFloat())
    }

    @RepeatedTest(25)
    fun testDot() {
        val size = random.nextInt(2048)

        val c1 = FloatVectorValue.random(size, this.random)
        val c2 = FloatVectorValue.random(size, this.random)

        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it].toDouble() })
        val c2p = ArrayRealVector(DoubleArray(c2.data.size) { c2.data[it].toDouble() })

        val dot = c1.dot(c2)
        val dotp = c1p.dotProduct(c2p)

        isApproximatelyTheSame(dotp.toFloat(), dot.value.toFloat())
    }

    @RepeatedTest(25)
    fun testNorm2() {
        val size = random.nextInt(2048)

        val c1 = FloatVectorValue.random(size, this.random)

        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it].toDouble() })

        val norm = c1.norm2()
        val normp = c1p.norm

        isApproximatelyTheSame(normp.toFloat(), norm.value)
    }
}