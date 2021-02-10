package org.vitrivr.cottontail.math

import org.apache.commons.math3.complex.Complex
import org.apache.commons.math3.complex.ComplexField
import org.apache.commons.math3.linear.ArrayFieldVector
import org.apache.commons.math3.linear.ArrayRealVector
import org.apache.commons.math3.linear.FieldVector
import org.apache.commons.math3.linear.FieldVectorPreservingVisitor
import org.junit.jupiter.api.Assertions
import org.vitrivr.cottontail.model.values.Complex32Value
import org.vitrivr.cottontail.model.values.Complex32VectorValue
import org.vitrivr.cottontail.model.values.Complex64Value
import org.vitrivr.cottontail.model.values.Complex64VectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import kotlin.math.abs

const val DELTA_COARSE = 5e-5
const val DELTA_FINE = 1e-14

fun isApproximatelyTheSame(expected: Float, actual: Float) {
    if (actual == 0.0f) {
        Assertions.assertEquals(expected, actual)
        return
    }
    val ratio = expected / actual
    Assertions.assertTrue( ratio > 1.0f - DELTA_COARSE && ratio < 1.0f + DELTA_COARSE,
            "Value $actual not approximately the same as expected $expected!")
}

fun isApproximatelyTheSame(expected: Double, actual: Double) {
    if (actual == 0.0) {
        Assertions.assertEquals(expected, actual)
        return
    }
    val ratio = expected / actual
    Assertions.assertTrue( ratio > 1.0f - DELTA_FINE && ratio < 1.0f + DELTA_FINE,
            "Value $actual not approximately the same as expected $expected!")
}

fun isApproximatelyTheSame(expected: Number, actual: Number) {
    val coarse = expected is Float
    val delta = if (coarse) DELTA_COARSE else DELTA_FINE
    val cond = if (expected == 0.0f || expected == 0.0) {
        val diff = abs(expected.toDouble() - actual.toDouble())
        diff < delta
    } else {
        val ratio = expected.toDouble() / actual.toDouble()
        ratio > 1.0f - delta && ratio < 1.0f + delta
    }
    Assertions.assertTrue(
        cond,
        "Value $actual not approximately the same as expected $expected! (delta: $delta)"
    )
}

fun equalVectors(expected: VectorValue<*>, actual: VectorValue<*>) {
    Assertions.assertEquals(expected.logicalSize, actual.logicalSize, "Vector sizes differ!")
    for (i in 0 until expected.logicalSize) {
        isApproximatelyTheSame(expected[i].real.value, actual[i].real.value)
        isApproximatelyTheSame(expected[i].imaginary.value, actual[i].imaginary.value)
    }
}

fun arrayFieldVectorFromVectorValue(vector: VectorValue<*>) : ArrayFieldVector<Complex> {
    val size = vector.logicalSize
    val vec = ArrayFieldVector(ComplexField.getInstance(), size)
    for (i in 0 until size) {
        vec.setEntry(i, Complex(vector[i].real.asDouble().value, vector[i].imaginary.asDouble().value))
    }
    return vec
}

fun complex32VectorFromFieldVector(vector: FieldVector<Complex>) : Complex32VectorValue {
    vector as ArrayFieldVector
    val complexs = vector.dataRef.map {
        Complex32Value(it.real, it.imaginary)
    }.toTypedArray()
    return Complex32VectorValue(complexs)
}

fun complex64VectorFromFieldVector(vector: FieldVector<Complex>) : Complex64VectorValue {
    vector as ArrayFieldVector
    val complexs = vector.dataRef.map {
        Complex64Value(it.real, it.imaginary)
    }.toTypedArray()
    return Complex64VectorValue(complexs)
}

fun absFromFromComplexFieldVector(vector: ArrayFieldVector<Complex>) : ArrayRealVector {
    val vec = ArrayRealVector(vector.dimension)
    vector.walkInOptimizedOrder(object : FieldVectorPreservingVisitor<Complex> {
        override fun end() : Complex {return Complex.ZERO}
        override fun start(dimension: Int, start: Int, end: Int) {return}
        override fun visit(index: Int, value: Complex) {vec.setEntry(index, value.abs())}
    })
    return vec
}

fun conjFromFromComplexFieldVector(vector: ArrayFieldVector<Complex>) : ArrayFieldVector<Complex> {
    val vec = ArrayFieldVector(ComplexField.getInstance(), vector.dimension)
    vector.walkInOptimizedOrder(object : FieldVectorPreservingVisitor<Complex> {
        override fun end() : Complex {return Complex.ZERO}
        override fun start(dimension: Int, start: Int, end: Int) {return}
        override fun visit(index: Int, value: Complex) {vec.setEntry(index, value.conjugate())}
    })
    return vec
}
