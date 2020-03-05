package ch.unibas.dmi.dbis.cottontail.server.grpc.helper

import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.values.*
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.init

import java.lang.IllegalArgumentException
import java.util.*

/**
 * Helper class to convert Kotlin data types to gRPC [Data] objects
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
object DataHelper {
    fun toData(value: Any?): CottontailGrpc.Data? = when (value) {
        is StringValue -> CottontailGrpc.Data.newBuilder().setStringData(value.value).build()
        is LongValue -> CottontailGrpc.Data.newBuilder().setLongData(value.value).build()
        is IntValue -> CottontailGrpc.Data.newBuilder().setIntData(value.value).build()
        is ShortValue -> CottontailGrpc.Data.newBuilder().setIntData(value.value.toInt()).build()
        is ByteValue -> CottontailGrpc.Data.newBuilder().setIntData(value.value.toInt()).build()
        is DoubleValue -> CottontailGrpc.Data.newBuilder().setDoubleData(value.value).build()
        is FloatValue -> CottontailGrpc.Data.newBuilder().setFloatData(value.value).build()
        is BooleanValue -> CottontailGrpc.Data.newBuilder().setBooleanData(value.value).build()
        is Complex32Value -> CottontailGrpc.Data.newBuilder().setComplex32Data(CottontailGrpc.Complex32.newBuilder().setReal(value.real.value).setImaginary(value.imaginary.value)).build()
        is Complex64Value -> CottontailGrpc.Data.newBuilder().setComplex64Data(CottontailGrpc.Complex64.newBuilder().setReal(value.real.value).setImaginary(value.imaginary.value)).build()
        is DoubleVectorValue -> CottontailGrpc.Data.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setDoubleVector(CottontailGrpc.DoubleVector.newBuilder().addAllVector(value.map { it.value }))).build()
        is FloatVectorValue -> CottontailGrpc.Data.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setFloatVector(CottontailGrpc.FloatVector.newBuilder().addAllVector(value.map { it.value }))).build()
        is LongVectorValue -> CottontailGrpc.Data.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setLongVector(CottontailGrpc.LongVector.newBuilder().addAllVector(value.map { it.value }))).build()
        is IntVectorValue -> CottontailGrpc.Data.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setIntVector(CottontailGrpc.IntVector.newBuilder().addAllVector(value.map { it.value }))).build()
        is Complex32VectorValue -> CottontailGrpc.Data.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setComplex32Vector(CottontailGrpc.Complex32Vector.newBuilder().addAllVector(value.map { CottontailGrpc.Complex32.newBuilder().setReal(it.real.value).setImaginary(it.imaginary.value).build() }))).build()
        is Complex64VectorValue -> CottontailGrpc.Data.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setComplex64Vector(CottontailGrpc.Complex64Vector.newBuilder().addAllVector(value.map { CottontailGrpc.Complex64.newBuilder().setReal(it.real.value).setImaginary(it.imaginary.value).build() }))).build()
        null -> CottontailGrpc.Data.newBuilder().setNullData(CottontailGrpc.Null.getDefaultInstance()).build()
        else -> throw IllegalArgumentException("The specified value cannot be converted to a gRPC Data object.")
    }
}

/**
 * Returns the value of [CottontailGrpc.Data] as String.
 *
 * @return [StringValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toStringValue(): StringValue? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> StringValue(this.booleanData.toString())
    CottontailGrpc.Data.DataCase.INTDATA -> StringValue(this.intData.toString())
    CottontailGrpc.Data.DataCase.LONGDATA -> StringValue(this.longData.toString())
    CottontailGrpc.Data.DataCase.FLOATDATA -> StringValue(this.floatData.toString())
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> StringValue(this.doubleData.toString())
    CottontailGrpc.Data.DataCase.STRINGDATA -> StringValue(this.stringData)
    CottontailGrpc.Data.DataCase.COMPLEX32DATA -> StringValue("${this.complex32Data.real}+i*${this.complex32Data.imaginary}")
    CottontailGrpc.Data.DataCase.COMPLEX64DATA -> StringValue("${this.complex64Data.real}+i*${this.complex64Data.imaginary}")
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of type VECTOR cannot be cast to STRING.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to STRING.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to DOUBLE.")
}

/**
 * Returns the value of [CottontailGrpc.Data] as Boolean.
 *
 * @return [BooleanValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toBooleanValue(): BooleanValue? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> BooleanValue(this.booleanData)
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.INTDATA -> throw QueryException.UnsupportedCastException("A value of type INT cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.LONGDATA -> throw QueryException.UnsupportedCastException("A value of type LONG cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.FLOATDATA -> throw QueryException.UnsupportedCastException("A value of type FLOAT cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> throw QueryException.UnsupportedCastException("A value of DOUBLE cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.STRINGDATA -> throw QueryException.UnsupportedCastException("A value of STRING cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to BOOLEAN.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to BOOLEAN.")
}

/**
 * Returns the value of [CottontailGrpc.Data] as Double.
 *
 * @return [DoubleValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toDoubleValue(): DoubleValue? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> DoubleValue(if (this.booleanData) { 1.0 } else { 0.0 })
    CottontailGrpc.Data.DataCase.INTDATA -> DoubleValue(this.intData.toDouble())
    CottontailGrpc.Data.DataCase.LONGDATA -> DoubleValue(this.longData.toDouble())
    CottontailGrpc.Data.DataCase.FLOATDATA -> DoubleValue(this.floatData.toDouble())
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> DoubleValue(this.doubleData)
    CottontailGrpc.Data.DataCase.STRINGDATA -> DoubleValue(this.stringData.toDoubleOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to DOUBLE."))
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to DOUBLE.")
    CottontailGrpc.Data.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to DOUBLE.")
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to DOUBLE.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to DOUBLE.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to DOUBLE.")
}

/**
 * Returns the value of [CottontailGrpc.Data] as Float.
 *
 * @return [FloatValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toFloatValue(): FloatValue? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> FloatValue(if (this.booleanData) { 1.0f } else { 0.0f })
    CottontailGrpc.Data.DataCase.INTDATA -> FloatValue(this.intData.toFloat())
    CottontailGrpc.Data.DataCase.LONGDATA -> FloatValue(this.longData.toFloat())
    CottontailGrpc.Data.DataCase.FLOATDATA -> FloatValue(this.floatData)
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> FloatValue(this.doubleData.toFloat())
    CottontailGrpc.Data.DataCase.STRINGDATA -> FloatValue(this.stringData.toFloatOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to FLOAT."))
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to FLOAT.")
    CottontailGrpc.Data.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to FLOAT.")
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to FLOAT.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to FLOAT.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to FLOAT.")
}

/**
 * Returns the value of [CottontailGrpc.Data] as Int.
 *
 * @return [ShortValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toShortValue(): ShortValue? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> ShortValue((if (this.booleanData) { 1 } else { 0 }).toShort())
    CottontailGrpc.Data.DataCase.INTDATA -> ShortValue(this.intData.toShort())
    CottontailGrpc.Data.DataCase.LONGDATA -> ShortValue(this.longData.toShort())
    CottontailGrpc.Data.DataCase.FLOATDATA -> ShortValue(this.floatData.toShort())
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> ShortValue(this.doubleData.toShort())
    CottontailGrpc.Data.DataCase.STRINGDATA -> ShortValue(this.stringData.toShortOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to SHORT."))
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to SHORT.")
    CottontailGrpc.Data.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to SHORT.")
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to SHORT.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to SHORT.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to SHORT.")
}

/**
 * Returns the value of [CottontailGrpc.Data] as Int.
 *
 * @return [ByteValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toByteValue(): ByteValue? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> ByteValue((if (this.booleanData) { 1 } else { 0 }).toByte())
    CottontailGrpc.Data.DataCase.INTDATA -> ByteValue(this.intData.toByte())
    CottontailGrpc.Data.DataCase.LONGDATA -> ByteValue(this.longData.toByte())
    CottontailGrpc.Data.DataCase.FLOATDATA -> ByteValue(this.floatData.toByte())
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> ByteValue(this.doubleData.toByte())
    CottontailGrpc.Data.DataCase.STRINGDATA -> ByteValue(this.stringData.toByteOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to BYTE."))
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to BYTE.")
    CottontailGrpc.Data.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to BYTE.")
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to BYTE.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to BYTE.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to BYTE.")
}

/**
 * Returns the value of [CottontailGrpc.Data] as [IntValue].
 *
 * @return [IntValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toIntValue(): IntValue? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> IntValue(if (this.booleanData) { 1 } else { 0 })
    CottontailGrpc.Data.DataCase.INTDATA -> IntValue(this.intData)
    CottontailGrpc.Data.DataCase.LONGDATA -> IntValue(this.longData.toInt())
    CottontailGrpc.Data.DataCase.FLOATDATA -> IntValue(this.floatData.toInt())
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> IntValue(this.doubleData.toInt())
    CottontailGrpc.Data.DataCase.STRINGDATA -> IntValue(this.stringData.toIntOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to INT."))
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to INT.")
    CottontailGrpc.Data.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to INT.")
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to INT.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to INT.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to INT.")
}

/**
 * Returns the value of [CottontailGrpc.Data] as [LongValue].
 *
 * @return [LongValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toLongValue(): LongValue? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> LongValue(if (this.booleanData) { 1L } else { 0L })
    CottontailGrpc.Data.DataCase.INTDATA -> LongValue(this.intData.toLong())
    CottontailGrpc.Data.DataCase.LONGDATA -> LongValue(this.longData)
    CottontailGrpc.Data.DataCase.FLOATDATA -> LongValue(this.floatData.toLong())
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> LongValue(this.doubleData.toLong())
    CottontailGrpc.Data.DataCase.STRINGDATA -> LongValue(this.stringData.toLongOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to LONG."))
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to LONG.")
    CottontailGrpc.Data.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to LONG.")
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to LONG.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to LONG.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to LONG.")
}

/**
 * Returns the value of [CottontailGrpc.Data] as [Complex32Value].
 *
 * @return [Complex32Value]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toComplex32Value(): Complex32Value? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> throw QueryException.UnsupportedCastException("A value of BOOLEAN cannot be cast to COMPLEX32.")
    CottontailGrpc.Data.DataCase.INTDATA -> Complex32Value(floatArrayOf(this.intData.toFloat(), 0.0f))
    CottontailGrpc.Data.DataCase.LONGDATA -> Complex32Value(floatArrayOf(this.longData.toFloat(), 0.0f))
    CottontailGrpc.Data.DataCase.FLOATDATA -> Complex32Value(floatArrayOf(this.floatData, 0.0f))
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> Complex32Value(floatArrayOf(this.doubleData.toFloat(), 0.0f))
    CottontailGrpc.Data.DataCase.STRINGDATA -> throw QueryException.UnsupportedCastException("A value of STRING cannot be cast to COMPLEX32.")
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.COMPLEX32DATA -> Complex32Value(floatArrayOf(this.complex32Data.real, this.complex32Data.imaginary))
    CottontailGrpc.Data.DataCase.COMPLEX64DATA -> Complex32Value(floatArrayOf(this.complex64Data.real.toFloat(), this.complex64Data.imaginary.toFloat())) // cave! precision
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to COMPLEX32.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to COMPLEX32.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to COMPLEX32.")
}

/**
 * Returns the value of [CottontailGrpc.Data] as [Complex64Value].
 *
 * @return [Complex64Value]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toComplex64Value(): Complex64Value? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> throw QueryException.UnsupportedCastException("A value of BOOLEAN cannot be cast to COMPLEX64.")
    CottontailGrpc.Data.DataCase.INTDATA -> Complex64Value(doubleArrayOf(this.intData.toDouble(), 0.0))
    CottontailGrpc.Data.DataCase.LONGDATA -> Complex64Value(doubleArrayOf(this.longData.toDouble(), 0.0))
    CottontailGrpc.Data.DataCase.FLOATDATA -> Complex64Value(doubleArrayOf(this.floatData.toDouble(), 0.0))
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> Complex64Value(doubleArrayOf(this.doubleData, 0.0))
    CottontailGrpc.Data.DataCase.STRINGDATA -> throw QueryException.UnsupportedCastException("A value of STRING cannot be cast to COMPLEX64.")
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.COMPLEX32DATA -> Complex64Value(doubleArrayOf(this.complex32Data.real.toDouble(), this.complex32Data.imaginary.toDouble()))
    CottontailGrpc.Data.DataCase.COMPLEX64DATA -> Complex64Value(doubleArrayOf(this.complex64Data.real, this.complex64Data.imaginary))
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to COMPLEX64.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to COMPLEX64.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to COMPLEX64.")
}

/**
 * Returns the value of [CottontailGrpc.Data] as [FloatVectorValue].
 *
 * @return [FloatVectorValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toFloatVectorValue(): FloatVectorValue? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> FloatVectorValue(FloatArray(1) { if (this.booleanData) { 1.0f } else { 0.0f } })
    CottontailGrpc.Data.DataCase.INTDATA -> FloatVectorValue(FloatArray(1) { this.intData.toFloat() })
    CottontailGrpc.Data.DataCase.LONGDATA -> FloatVectorValue(FloatArray(1) { this.longData.toFloat() })
    CottontailGrpc.Data.DataCase.FLOATDATA -> FloatVectorValue(FloatArray(1) { this.floatData })
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> FloatVectorValue(FloatArray(1) { this.doubleData.toFloat() })
    CottontailGrpc.Data.DataCase.STRINGDATA -> FloatVectorValue(FloatArray(1) { this.stringData.toFloatOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[FLOAT].") })
    CottontailGrpc.Data.DataCase.VECTORDATA -> this.vectorData.toFloatVectorValue()
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to VECTOR.") /* TODO: Change. */
    CottontailGrpc.Data.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to VECTOR.")  /* TODO: Change. */
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[FLOAT].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[FLOAT].")
}


/**
 * Returns the value of [CottontailGrpc.Data] as [DoubleVectorValue].
 *
 * @return [DoubleVectorValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toDoubleVectorValue(): DoubleVectorValue? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> DoubleVectorValue(DoubleArray(1) { if (this.booleanData) { 1.0 } else { 0.0 } })
    CottontailGrpc.Data.DataCase.INTDATA -> DoubleVectorValue(DoubleArray(1) { this.intData.toDouble() })
    CottontailGrpc.Data.DataCase.LONGDATA -> DoubleVectorValue(DoubleArray(1) { this.longData.toDouble() })
    CottontailGrpc.Data.DataCase.FLOATDATA -> DoubleVectorValue(DoubleArray(1) { this.floatData.toDouble() })
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> DoubleVectorValue(DoubleArray(1) { this.doubleData })
    CottontailGrpc.Data.DataCase.STRINGDATA -> DoubleVectorValue(DoubleArray(1) { this.stringData.toDoubleOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[DOUBLE].") })
    CottontailGrpc.Data.DataCase.VECTORDATA -> this.vectorData.toDoubleVectorValue()
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to VECTOR[DOUBLE].")
    CottontailGrpc.Data.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to VECTOR[DOUBLE].")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[DOUBLE].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[DOUBLE].")
}

/**
 * Returns the value of [CottontailGrpc.Data] as [LongVectorValue].
 *
 * @return [LongVectorValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toLongVectorValue(): LongVectorValue? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> LongVectorValue(LongArray(1) { if (this.booleanData) { 1L } else { 0L } })
    CottontailGrpc.Data.DataCase.INTDATA -> LongVectorValue(LongArray(1) { this.intData.toLong() })
    CottontailGrpc.Data.DataCase.LONGDATA -> LongVectorValue(LongArray(1) { this.longData })
    CottontailGrpc.Data.DataCase.FLOATDATA -> LongVectorValue(LongArray(1) { this.floatData.toLong() })
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> LongVectorValue(LongArray(1) { this.doubleData.toLong() })
    CottontailGrpc.Data.DataCase.STRINGDATA -> LongVectorValue(LongArray(1) { this.stringData.toLongOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[LONG].") })
    CottontailGrpc.Data.DataCase.VECTORDATA -> this.vectorData.toLongVectorValue()
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to VECTOR[LONG].")
    CottontailGrpc.Data.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to VECTOR[LONG].")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[LONG].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[LONG].")
}

/**
 * Returns the value of [CottontailGrpc.Data] as [IntVectorValue].
 *
 * @return [IntVectorValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toIntVectorValue(): IntVectorValue? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> IntVectorValue(IntArray(1) { if (this.booleanData) { 1 } else { 0 } })
    CottontailGrpc.Data.DataCase.INTDATA -> IntVectorValue(IntArray(1) { this.intData })
    CottontailGrpc.Data.DataCase.LONGDATA -> IntVectorValue(IntArray(1) { this.longData.toInt() })
    CottontailGrpc.Data.DataCase.FLOATDATA -> IntVectorValue(IntArray(1) { this.floatData.toInt() })
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> IntVectorValue(IntArray(1) { this.doubleData.toInt() })
    CottontailGrpc.Data.DataCase.STRINGDATA -> IntVectorValue(IntArray(1) { this.stringData.toIntOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[INT].") })
    CottontailGrpc.Data.DataCase.VECTORDATA -> this.vectorData.toIntVectorValue()
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to VECTOR[INT].")
    CottontailGrpc.Data.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to VECTOR[INT].")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[INT].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[INT].")
}

/**
 *
 * Returns the value of [CottontailGrpc.Data] as [BooleanVectorValue].
 *
 * @return [BooleanVectorValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toBooleanVectorValue(): BooleanVectorValue? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> BooleanVectorValue(BitSet(1).init { this.booleanData })
    CottontailGrpc.Data.DataCase.INTDATA -> BooleanVectorValue(BitSet(1).init { this.intData > 0 })
    CottontailGrpc.Data.DataCase.LONGDATA -> BooleanVectorValue(BitSet(1).init { this.longData > 0 })
    CottontailGrpc.Data.DataCase.FLOATDATA -> BooleanVectorValue(BitSet(1).init { this.floatData > 0f })
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> BooleanVectorValue(BitSet(1).init { this.doubleData > 0.0 })
    CottontailGrpc.Data.DataCase.STRINGDATA -> BooleanVectorValue(BitSet(1).init { this.stringData == "true" })
    CottontailGrpc.Data.DataCase.VECTORDATA -> this.vectorData.toBooleanVectorValue()
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to VECTOR[BOOL].")
    CottontailGrpc.Data.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to VECTOR[BOOL].")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[BOOL].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[BOOL].")
}

/**
 * Returns the value of [CottontailGrpc.Data] as [Complex32VectorValue].
 *
 * @return [Complex32VectorValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toComplex32VectorValue(): Complex32VectorValue? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> throw QueryException.UnsupportedCastException("A value of BOOL cannot be cast to VECTOR[COMPLEX32].")
    CottontailGrpc.Data.DataCase.INTDATA -> Complex32VectorValue(arrayOf(Complex32Value(this.intData)))
    CottontailGrpc.Data.DataCase.LONGDATA -> Complex32VectorValue(arrayOf(Complex32Value(this.longData)))
    CottontailGrpc.Data.DataCase.FLOATDATA -> Complex32VectorValue(arrayOf(Complex32Value(this.floatData)))
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> Complex32VectorValue(arrayOf(Complex32Value(this.doubleData)))
    CottontailGrpc.Data.DataCase.STRINGDATA -> throw QueryException.UnsupportedCastException("A value of STRING cannot be cast to VECTOR[COMPLEX32].")
    CottontailGrpc.Data.DataCase.VECTORDATA -> this.vectorData.toComplex32VectorValue()
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.COMPLEX32DATA -> Complex32VectorValue(arrayOf(Complex32Value(FloatValue(this.complex32Data.real), FloatValue(this.complex32Data.imaginary))))
    CottontailGrpc.Data.DataCase.COMPLEX64DATA -> Complex32VectorValue(arrayOf(Complex32Value(FloatValue(this.complex64Data.real.toFloat()), FloatValue(this.complex64Data.imaginary.toFloat())))) // cave! precision!
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[COMPLEX32].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[COMPLEX32].")
}

/**
 * Returns the value of [CottontailGrpc.Data] as [Complex64VectorValue].
 *
 * @return [Complex64VectorValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toComplex64VectorValue(): Complex64VectorValue? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> throw QueryException.UnsupportedCastException("A value of BOOL cannot be cast to VECTOR[COMPLEX64].")
    CottontailGrpc.Data.DataCase.INTDATA -> Complex64VectorValue(arrayOf(Complex64Value(this.intData)))
    CottontailGrpc.Data.DataCase.LONGDATA -> Complex64VectorValue(arrayOf(Complex64Value(this.longData)))
    CottontailGrpc.Data.DataCase.FLOATDATA -> Complex64VectorValue(arrayOf(Complex64Value(this.floatData)))
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> Complex64VectorValue(arrayOf(Complex64Value(this.doubleData)))
    CottontailGrpc.Data.DataCase.STRINGDATA -> throw QueryException.UnsupportedCastException("A value of STRING cannot be cast to VECTOR[COMPLEX64].")
    CottontailGrpc.Data.DataCase.VECTORDATA -> this.vectorData.toComplex64VectorValue()
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.COMPLEX32DATA -> Complex64VectorValue(arrayOf(Complex64Value(this.complex32Data.real, this.complex32Data.imaginary)))
    CottontailGrpc.Data.DataCase.COMPLEX64DATA -> Complex64VectorValue(arrayOf(Complex64Value(this.complex64Data.real, this.complex64Data.imaginary)))
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[COMPLEX64].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[COMPLEX64].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as [DoubleArray].
 *
 * @return [DoubleArray] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Vector.toDoubleVectorValue(): DoubleVectorValue = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> DoubleVectorValue(this.doubleVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> DoubleVectorValue(this.floatVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> DoubleVectorValue(this.longVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> DoubleVectorValue(this.intVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> DoubleVectorValue(this.boolVector.vectorList.map { if (it) 1.0 else 0.0  })
    CottontailGrpc.Vector.VectorDataCase.COMPLEX32VECTOR -> throw QueryException.UnsupportedCastException("A value of VECTOR[COMPLEX32] cannot be cast to VECTOR[DOUBLE].")
    CottontailGrpc.Vector.VectorDataCase.COMPLEX64VECTOR -> throw QueryException.UnsupportedCastException("A value of VECTOR[COMPLEX64] cannot be cast to VECTOR[DOUBLE].")
    CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[DOUBLE].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[DOUBLE].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as [FloatVectorValue].
 *
 * @return [FloatVectorValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Vector.toFloatVectorValue(): FloatVectorValue = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> FloatVectorValue(this.doubleVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> FloatVectorValue(this.floatVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> FloatVectorValue(this.longVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> FloatVectorValue(this.intVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> FloatVectorValue(this.boolVector.vectorList.map { if (it) 1.0f else 0.0f  })
    CottontailGrpc.Vector.VectorDataCase.COMPLEX32VECTOR -> throw QueryException.UnsupportedCastException("A value of VECTOR[COMPLEX32] cannot be cast to VECTOR[FLOAT].")
    CottontailGrpc.Vector.VectorDataCase.COMPLEX64VECTOR -> throw QueryException.UnsupportedCastException("A value of VECTOR[COMPLEX64] cannot be cast to VECTOR[FLOAT].")
    CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[FLOAT].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[FLOAT].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as [LongVectorValue].
 *
 * @return [LongVectorValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Vector.toLongVectorValue(): LongVectorValue = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> LongVectorValue(this.doubleVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> LongVectorValue(this.floatVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> LongVectorValue(this.longVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> LongVectorValue(this.intVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> LongVectorValue(this.boolVector.vectorList.map { if (it) 1L else 0L  })
    CottontailGrpc.Vector.VectorDataCase.COMPLEX32VECTOR -> throw QueryException.UnsupportedCastException("A value of VECTOR[COMPLEX32] cannot be cast to VECTOR[LONG].")
    CottontailGrpc.Vector.VectorDataCase.COMPLEX64VECTOR -> throw QueryException.UnsupportedCastException("A value of VECTOR[COMPLEX64] cannot be cast to VECTOR[LONG].")
    CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[LONG].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[LONG].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as [IntArray].
 *
 * @return [IntArray] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Vector.toIntVectorValue(): IntVectorValue = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> IntVectorValue(this.doubleVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> IntVectorValue(this.floatVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> IntVectorValue(this.longVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> IntVectorValue(this.intVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> IntVectorValue(this.boolVector.vectorList.map { if (it) 1 else 0  })
    CottontailGrpc.Vector.VectorDataCase.COMPLEX32VECTOR -> throw QueryException.UnsupportedCastException("A value of VECTOR[COMPLEX32] cannot be cast to VECTOR[INT].")
    CottontailGrpc.Vector.VectorDataCase.COMPLEX64VECTOR -> throw QueryException.UnsupportedCastException("A value of VECTOR[COMPLEX64] cannot be cast to VECTOR[INT].")
    CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[INT].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[INT].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as [BitSet].
 *
 * @return [BitSet] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Vector.toBooleanVectorValue(): BooleanVectorValue = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> BooleanVectorValue(this.doubleVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> BooleanVectorValue(this.floatVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> BooleanVectorValue(this.longVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> BooleanVectorValue(this.intVector.vectorList)
    CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> BooleanVectorValue(this.boolVector.vectorList.toTypedArray())
    CottontailGrpc.Vector.VectorDataCase.COMPLEX32VECTOR -> throw QueryException.UnsupportedCastException("A value of VECTOR[COMPLEX32] cannot be cast to VECTOR[BOOL].")
    CottontailGrpc.Vector.VectorDataCase.COMPLEX64VECTOR -> throw QueryException.UnsupportedCastException("A value of VECTOR[COMPLEX64] cannot be cast to VECTOR[BOOL].")
    CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[BOOL].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[BOOL].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as [Complex32VectorValue].
 *
 * @return [Complex32VectorValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Vector.toComplex32VectorValue(): Complex32VectorValue = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> Complex32VectorValue(Array(this.doubleVector.vectorList.size) { Complex32Value(this.doubleVector.vectorList[it], 0.0f) })
    CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> Complex32VectorValue(Array(this.floatVector.vectorList.size) { Complex32Value(this.floatVector.vectorList[it], 0.0f) })
    CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> Complex32VectorValue(Array(this.longVector.vectorList.size) { Complex32Value(this.longVector.vectorList[it],0.0f) })
    CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> Complex32VectorValue(Array(this.intVector.vectorList.size) { Complex32Value(this.intVector.vectorList[it], 0.0f) })
    CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> throw QueryException.UnsupportedCastException("A value of BOOL cannot be cast to VECTOR[COMPLEX32].")
    CottontailGrpc.Vector.VectorDataCase.COMPLEX32VECTOR -> Complex32VectorValue(Array(this.complex32Vector.vectorList.size) { Complex32Value(FloatValue(this.complex32Vector.vectorList[it].real), FloatValue(this.complex32Vector.vectorList[it].imaginary)) })
    CottontailGrpc.Vector.VectorDataCase.COMPLEX64VECTOR -> Complex32VectorValue(Array(this.complex64Vector.vectorList.size) { Complex32Value(FloatValue(this.complex64Vector.vectorList[it].real), FloatValue(this.complex64Vector.vectorList[it].imaginary))} ) // caveat! precision
    CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[COMPLEX32].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[COMPLEX32].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as [Complex64VectorValue].
 *
 * @return [Complex64VectorValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Vector.toComplex64VectorValue(): Complex64VectorValue = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> Complex64VectorValue(Array(this.doubleVector.vectorList.size) { Complex64Value(this.doubleVector.vectorList[it], 0.0) })
    CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> Complex64VectorValue(Array(this.floatVector.vectorList.size) { Complex64Value(this.floatVector.vectorList[it], 0.0) })
    CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> Complex64VectorValue(Array(this.longVector.vectorList.size) { Complex64Value(this.longVector.vectorList[it], 0.0) })
    CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> Complex64VectorValue(Array(this.intVector.vectorList.size) { Complex64Value(this.intVector.vectorList[it], 0.0) })
    CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> throw QueryException.UnsupportedCastException("A value of BOOL cannot be cast to VECTOR[COMPLEX64].")
    CottontailGrpc.Vector.VectorDataCase.COMPLEX32VECTOR -> Complex64VectorValue(Array(this.complex32Vector.vectorList.size) { Complex64Value(this.complex32Vector.vectorList[it].real, this.complex32Vector.vectorList[it].imaginary) })
    CottontailGrpc.Vector.VectorDataCase.COMPLEX64VECTOR -> Complex64VectorValue(Array(this.complex64Vector.vectorList.size) { Complex64Value(this.complex64Vector.vectorList[it].real, this.complex64Vector.vectorList[it].imaginary) })
    CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[COMPLEX64].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[COMPLEX64].")
}