package org.vitrivr.cottontail.server.grpc.helper
import org.vitrivr.cottontail.database.column.*
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value
import java.util.*

/**
 * Helpers to convert Kotlin data types to gRPC [CottontailGrpc.Literal] and vice versa.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */

/**
 * Converts this [Value] to the corresponding [CottontailGrpc.Literal] object and returns it.
 *
 * @return [CottontailGrpc.Literal]
 */
fun Value.toLiteral(): CottontailGrpc.Literal = when (this) {
    is StringValue -> CottontailGrpc.Literal.newBuilder().setStringData(this.value).build()
    is LongValue -> CottontailGrpc.Literal.newBuilder().setLongData(this.value).build()
    is IntValue -> CottontailGrpc.Literal.newBuilder().setIntData(this.value).build()
    is ShortValue -> CottontailGrpc.Literal.newBuilder().setIntData(this.value.toInt()).build()
    is ByteValue -> CottontailGrpc.Literal.newBuilder().setIntData(this.value.toInt()).build()
    is DoubleValue -> CottontailGrpc.Literal.newBuilder().setDoubleData(this.value).build()
    is FloatValue -> CottontailGrpc.Literal.newBuilder().setFloatData(this.value).build()
    is BooleanValue -> CottontailGrpc.Literal.newBuilder().setBooleanData(this.value).build()
    is Complex32Value -> CottontailGrpc.Literal.newBuilder().setComplex32Data(CottontailGrpc.Complex32.newBuilder().setReal(this.real.value).setImaginary(this.imaginary.value)).build()
    is Complex64Value -> CottontailGrpc.Literal.newBuilder().setComplex64Data(CottontailGrpc.Complex64.newBuilder().setReal(this.real.value).setImaginary(this.imaginary.value)).build()
    is DoubleVectorValue -> CottontailGrpc.Literal.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setDoubleVector(CottontailGrpc.DoubleVector.newBuilder().addAllVector(this.map { it.value }))).build()
    is FloatVectorValue -> CottontailGrpc.Literal.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setFloatVector(CottontailGrpc.FloatVector.newBuilder().addAllVector(this.map { it.value }))).build()
    is LongVectorValue -> CottontailGrpc.Literal.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setLongVector(CottontailGrpc.LongVector.newBuilder().addAllVector(this.map { it.value }))).build()
    is IntVectorValue -> CottontailGrpc.Literal.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setIntVector(CottontailGrpc.IntVector.newBuilder().addAllVector(this.map { it.value }))).build()
    is BooleanVectorValue -> CottontailGrpc.Literal.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setBoolVector(CottontailGrpc.BoolVector.newBuilder().addAllVector(this.indices.map { this.getAsBool(it) }))).build()
    is Complex32VectorValue -> CottontailGrpc.Literal.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setComplex32Vector(CottontailGrpc.Complex32Vector.newBuilder().addAllVector(this.map { CottontailGrpc.Complex32.newBuilder().setReal(it.real.value).setImaginary(it.imaginary.value).build() }))).build()
    is Complex64VectorValue -> CottontailGrpc.Literal.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setComplex64Vector(CottontailGrpc.Complex64Vector.newBuilder().addAllVector(this.map { CottontailGrpc.Complex64.newBuilder().setReal(it.real.value).setImaginary(it.imaginary.value).build() }))).build()
    else -> throw IllegalArgumentException("The specified value cannot be converted to a gRPC Data object.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [Value].
 *
 * @return [T]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun <T : Value> CottontailGrpc.Literal.toValue(column: ColumnDef<T>): T? {
    val cast = when (column.type) {
        is DoubleColumnType -> this.toDoubleValue()
        is FloatColumnType -> this.toFloatValue()
        is BooleanColumnType -> this.toBooleanValue()
        is ByteColumnType -> this.toByteValue()
        is ShortColumnType -> this.toShortValue()
        is IntColumnType -> this.toIntValue()
        is LongColumnType -> this.toLongValue()
        is StringColumnType -> this.toStringValue()
        is Complex32ColumnType -> this.toComplex32Value()
        is Complex64ColumnType -> this.toComplex64Value()
        is IntVectorColumnType -> this.toIntVectorValue()
        is LongVectorColumnType -> this.toLongVectorValue()
        is FloatVectorColumnType -> this.toFloatVectorValue()
        is DoubleVectorColumnType -> this.toDoubleVectorValue()
        is BooleanVectorColumnType -> this.toBooleanVectorValue()
        is Complex32VectorColumnType -> this.toComplex32VectorValue()
        is Complex64VectorColumnType -> this.toComplex64VectorValue()
    }
    return column.type.cast(cast) ?: if (column.nullable) {
        null
    } else {
        throw QueryException.UnsupportedCastException("A value of NULL cannot be cast a type that isn't nullable.")
    }
}

/**
 * Returns the value of [CottontailGrpc.Literal] as String.
 *
 * @return [StringValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Literal.toStringValue(): StringValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> StringValue(this.booleanData.toString())
    CottontailGrpc.Literal.DataCase.INTDATA -> StringValue(this.intData.toString())
    CottontailGrpc.Literal.DataCase.LONGDATA -> StringValue(this.longData.toString())
    CottontailGrpc.Literal.DataCase.FLOATDATA -> StringValue(this.floatData.toString())
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> StringValue(this.doubleData.toString())
    CottontailGrpc.Literal.DataCase.STRINGDATA -> StringValue(this.stringData)
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> StringValue("${this.complex32Data.real}+i*${this.complex32Data.imaginary}")
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> StringValue("${this.complex64Data.real}+i*${this.complex64Data.imaginary}")
    CottontailGrpc.Literal.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of type VECTOR cannot be cast to STRING.")
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to STRING.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as Boolean.
 *
 * @return [BooleanValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Literal.toBooleanValue(): BooleanValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> BooleanValue(this.booleanData)
    CottontailGrpc.Literal.DataCase.INTDATA -> throw QueryException.UnsupportedCastException("A value of type INT cannot be cast to BOOLEAN.")
    CottontailGrpc.Literal.DataCase.LONGDATA -> throw QueryException.UnsupportedCastException("A value of type LONG cannot be cast to BOOLEAN.")
    CottontailGrpc.Literal.DataCase.FLOATDATA -> throw QueryException.UnsupportedCastException("A value of type FLOAT cannot be cast to BOOLEAN.")
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> throw QueryException.UnsupportedCastException("A value of DOUBLE cannot be cast to BOOLEAN.")
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to BOOLEAN.")
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to BOOLEAN.")
    CottontailGrpc.Literal.DataCase.STRINGDATA -> throw QueryException.UnsupportedCastException("A value of STRING cannot be cast to BOOLEAN.")
    CottontailGrpc.Literal.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to BOOLEAN.")
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to BOOLEAN.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as Double.
 *
 * @return [DoubleValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Literal.toDoubleValue(): DoubleValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> DoubleValue(if (this.booleanData) {
        1.0
    } else {
        0.0
    })
    CottontailGrpc.Literal.DataCase.INTDATA -> DoubleValue(this.intData.toDouble())
    CottontailGrpc.Literal.DataCase.LONGDATA -> DoubleValue(this.longData.toDouble())
    CottontailGrpc.Literal.DataCase.FLOATDATA -> DoubleValue(this.floatData.toDouble())
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> DoubleValue(this.doubleData)
    CottontailGrpc.Literal.DataCase.STRINGDATA -> DoubleValue(this.stringData.toDoubleOrNull()
            ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to DOUBLE."))
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to DOUBLE.")
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to DOUBLE.")
    CottontailGrpc.Literal.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to DOUBLE.")
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to DOUBLE.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as Float.
 *
 * @return [FloatValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Literal.toFloatValue(): FloatValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> FloatValue(if (this.booleanData) {
        1.0f
    } else {
        0.0f
    })
    CottontailGrpc.Literal.DataCase.INTDATA -> FloatValue(this.intData.toFloat())
    CottontailGrpc.Literal.DataCase.LONGDATA -> FloatValue(this.longData.toFloat())
    CottontailGrpc.Literal.DataCase.FLOATDATA -> FloatValue(this.floatData)
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> FloatValue(this.doubleData.toFloat())
    CottontailGrpc.Literal.DataCase.STRINGDATA -> FloatValue(this.stringData.toFloatOrNull()
            ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to FLOAT."))
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to FLOAT.")
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to FLOAT.")
    CottontailGrpc.Literal.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to FLOAT.")
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to FLOAT.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as Int.
 *
 * @return [ShortValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Literal.toShortValue(): ShortValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> ShortValue((if (this.booleanData) {
        1
    } else {
        0
    }).toShort())
    CottontailGrpc.Literal.DataCase.INTDATA -> ShortValue(this.intData.toShort())
    CottontailGrpc.Literal.DataCase.LONGDATA -> ShortValue(this.longData.toShort())
    CottontailGrpc.Literal.DataCase.FLOATDATA -> ShortValue(this.floatData.toInt().toShort())
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> ShortValue(this.doubleData.toInt().toShort())
    CottontailGrpc.Literal.DataCase.STRINGDATA -> ShortValue(this.stringData.toShortOrNull()
            ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to SHORT."))
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to SHORT.")
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to SHORT.")
    CottontailGrpc.Literal.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to SHORT.")
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to SHORT.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as Int.
 *
 * @return [ByteValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Literal.toByteValue(): ByteValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> ByteValue((if (this.booleanData) {
        1
    } else {
        0
    }).toByte())
    CottontailGrpc.Literal.DataCase.INTDATA -> ByteValue(this.intData.toByte())
    CottontailGrpc.Literal.DataCase.LONGDATA -> ByteValue(this.longData.toByte())
    CottontailGrpc.Literal.DataCase.FLOATDATA -> ByteValue(this.floatData.toInt().toByte())
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> ByteValue(this.doubleData.toInt().toByte())
    CottontailGrpc.Literal.DataCase.STRINGDATA -> ByteValue(this.stringData.toByteOrNull()
            ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to BYTE."))
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to BYTE.")
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to BYTE.")
    CottontailGrpc.Literal.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to BYTE.")
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to BYTE.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [IntValue].
 *
 * @return [IntValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Literal.toIntValue(): IntValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> IntValue(if (this.booleanData) {
        1
    } else {
        0
    })
    CottontailGrpc.Literal.DataCase.INTDATA -> IntValue(this.intData)
    CottontailGrpc.Literal.DataCase.LONGDATA -> IntValue(this.longData.toInt())
    CottontailGrpc.Literal.DataCase.FLOATDATA -> IntValue(this.floatData.toInt())
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> IntValue(this.doubleData.toInt())
    CottontailGrpc.Literal.DataCase.STRINGDATA -> IntValue(this.stringData.toIntOrNull()
            ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to INT."))
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to INT.")
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to INT.")
    CottontailGrpc.Literal.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to INT.")
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to INT.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [LongValue].
 *
 * @return [LongValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Literal.toLongValue(): LongValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> LongValue(if (this.booleanData) {
        1L
    } else {
        0L
    })
    CottontailGrpc.Literal.DataCase.INTDATA -> LongValue(this.intData.toLong())
    CottontailGrpc.Literal.DataCase.LONGDATA -> LongValue(this.longData)
    CottontailGrpc.Literal.DataCase.FLOATDATA -> LongValue(this.floatData.toLong())
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> LongValue(this.doubleData.toLong())
    CottontailGrpc.Literal.DataCase.STRINGDATA -> LongValue(this.stringData.toLongOrNull()
            ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to LONG."))
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to LONG.")
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to LONG.")
    CottontailGrpc.Literal.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to LONG.")
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to LONG.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [Complex32Value].
 *
 * @return [Complex32Value]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Literal.toComplex32Value(): Complex32Value? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> throw QueryException.UnsupportedCastException("A value of BOOLEAN cannot be cast to COMPLEX32.")
    CottontailGrpc.Literal.DataCase.INTDATA -> Complex32Value(floatArrayOf(this.intData.toFloat(), 0.0f))
    CottontailGrpc.Literal.DataCase.LONGDATA -> Complex32Value(floatArrayOf(this.longData.toFloat(), 0.0f))
    CottontailGrpc.Literal.DataCase.FLOATDATA -> Complex32Value(floatArrayOf(this.floatData, 0.0f))
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> Complex32Value(floatArrayOf(this.doubleData.toFloat(), 0.0f))
    CottontailGrpc.Literal.DataCase.STRINGDATA -> throw QueryException.UnsupportedCastException("A value of STRING cannot be cast to COMPLEX32.")
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> Complex32Value(floatArrayOf(this.complex32Data.real, this.complex32Data.imaginary))
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> Complex32Value(floatArrayOf(this.complex64Data.real.toFloat(), this.complex64Data.imaginary.toFloat())) // cave! precision
    CottontailGrpc.Literal.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to COMPLEX32.")
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to COMPLEX32.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [Complex64Value].
 *
 * @return [Complex64Value]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Literal.toComplex64Value(): Complex64Value? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> throw QueryException.UnsupportedCastException("A value of BOOLEAN cannot be cast to COMPLEX64.")
    CottontailGrpc.Literal.DataCase.INTDATA -> Complex64Value(doubleArrayOf(this.intData.toDouble(), 0.0))
    CottontailGrpc.Literal.DataCase.LONGDATA -> Complex64Value(doubleArrayOf(this.longData.toDouble(), 0.0))
    CottontailGrpc.Literal.DataCase.FLOATDATA -> Complex64Value(doubleArrayOf(this.floatData.toDouble(), 0.0))
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> Complex64Value(doubleArrayOf(this.doubleData, 0.0))
    CottontailGrpc.Literal.DataCase.STRINGDATA -> throw QueryException.UnsupportedCastException("A value of STRING cannot be cast to COMPLEX64.")
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> Complex64Value(doubleArrayOf(this.complex32Data.real.toDouble(), this.complex32Data.imaginary.toDouble()))
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> Complex64Value(doubleArrayOf(this.complex64Data.real, this.complex64Data.imaginary))
    CottontailGrpc.Literal.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to COMPLEX64.")
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to COMPLEX64.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [FloatVectorValue].
 *
 * @return [FloatVectorValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Literal.toFloatVectorValue(): FloatVectorValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> FloatVectorValue(FloatArray(1) {
        if (this.booleanData) {
            1.0f
        } else {
            0.0f
        }
    })
    CottontailGrpc.Literal.DataCase.INTDATA -> FloatVectorValue(FloatArray(1) { this.intData.toFloat() })
    CottontailGrpc.Literal.DataCase.LONGDATA -> FloatVectorValue(FloatArray(1) { this.longData.toFloat() })
    CottontailGrpc.Literal.DataCase.FLOATDATA -> FloatVectorValue(FloatArray(1) { this.floatData })
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> FloatVectorValue(FloatArray(1) { this.doubleData.toFloat() })
    CottontailGrpc.Literal.DataCase.STRINGDATA -> FloatVectorValue(FloatArray(1) {
        this.stringData.toFloatOrNull()
                ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[FLOAT].")
    })
    CottontailGrpc.Literal.DataCase.VECTORDATA -> this.vectorData.toFloatVectorValue()
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to VECTOR.") /* TODO: Change. */
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to VECTOR.")  /* TODO: Change. */
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[FLOAT].")
}


/**
 * Returns the value of [CottontailGrpc.Literal] as [DoubleVectorValue].
 *
 * @return [DoubleVectorValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Literal.toDoubleVectorValue(): DoubleVectorValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> DoubleVectorValue(DoubleArray(1) {
        if (this.booleanData) {
            1.0
        } else {
            0.0
        }
    })
    CottontailGrpc.Literal.DataCase.INTDATA -> DoubleVectorValue(DoubleArray(1) { this.intData.toDouble() })
    CottontailGrpc.Literal.DataCase.LONGDATA -> DoubleVectorValue(DoubleArray(1) { this.longData.toDouble() })
    CottontailGrpc.Literal.DataCase.FLOATDATA -> DoubleVectorValue(DoubleArray(1) { this.floatData.toDouble() })
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> DoubleVectorValue(DoubleArray(1) { this.doubleData })
    CottontailGrpc.Literal.DataCase.STRINGDATA -> DoubleVectorValue(DoubleArray(1) {
        this.stringData.toDoubleOrNull()
                ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[DOUBLE].")
    })
    CottontailGrpc.Literal.DataCase.VECTORDATA -> this.vectorData.toDoubleVectorValue()
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to VECTOR[DOUBLE].")
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to VECTOR[DOUBLE].")
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[DOUBLE].")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [LongVectorValue].
 *
 * @return [LongVectorValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Literal.toLongVectorValue(): LongVectorValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> LongVectorValue(LongArray(1) {
        if (this.booleanData) {
            1L
        } else {
            0L
        }
    })
    CottontailGrpc.Literal.DataCase.INTDATA -> LongVectorValue(LongArray(1) { this.intData.toLong() })
    CottontailGrpc.Literal.DataCase.LONGDATA -> LongVectorValue(LongArray(1) { this.longData })
    CottontailGrpc.Literal.DataCase.FLOATDATA -> LongVectorValue(LongArray(1) { this.floatData.toLong() })
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> LongVectorValue(LongArray(1) { this.doubleData.toLong() })
    CottontailGrpc.Literal.DataCase.STRINGDATA -> LongVectorValue(LongArray(1) {
        this.stringData.toLongOrNull()
                ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[LONG].")
    })
    CottontailGrpc.Literal.DataCase.VECTORDATA -> this.vectorData.toLongVectorValue()
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to VECTOR[LONG].")
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to VECTOR[LONG].")
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[LONG].")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [IntVectorValue].
 *
 * @return [IntVectorValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Literal.toIntVectorValue(): IntVectorValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> IntVectorValue(IntArray(1) {
        if (this.booleanData) {
            1
        } else {
            0
        }
    })
    CottontailGrpc.Literal.DataCase.INTDATA -> IntVectorValue(IntArray(1) { this.intData })
    CottontailGrpc.Literal.DataCase.LONGDATA -> IntVectorValue(IntArray(1) { this.longData.toInt() })
    CottontailGrpc.Literal.DataCase.FLOATDATA -> IntVectorValue(IntArray(1) { this.floatData.toInt() })
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> IntVectorValue(IntArray(1) { this.doubleData.toInt() })
    CottontailGrpc.Literal.DataCase.STRINGDATA -> IntVectorValue(IntArray(1) {
        this.stringData.toIntOrNull()
                ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[INT].")
    })
    CottontailGrpc.Literal.DataCase.VECTORDATA -> this.vectorData.toIntVectorValue()
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to VECTOR[INT].")
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to VECTOR[INT].")
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[INT].")
}

/**
 *
 * Returns the value of [CottontailGrpc.Literal] as [BooleanVectorValue].
 *
 * @return [BooleanVectorValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Literal.toBooleanVectorValue(): BooleanVectorValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> BooleanVectorValue(BooleanArray(1) { this.booleanData })
    CottontailGrpc.Literal.DataCase.INTDATA -> BooleanVectorValue(BooleanArray(1) { this.intData > 0 })
    CottontailGrpc.Literal.DataCase.LONGDATA -> BooleanVectorValue(BooleanArray(1) { this.longData > 0 })
    CottontailGrpc.Literal.DataCase.FLOATDATA -> BooleanVectorValue(BooleanArray(1) { this.floatData > 0f })
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> BooleanVectorValue(BooleanArray(1) { this.doubleData > 0.0 })
    CottontailGrpc.Literal.DataCase.STRINGDATA -> BooleanVectorValue(BooleanArray(1) { this.stringData == "true" })
    CottontailGrpc.Literal.DataCase.VECTORDATA -> this.vectorData.toBooleanVectorValue()
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX32 cannot be cast to VECTOR[BOOL].")
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> throw QueryException.UnsupportedCastException("A value of COMPLEX64 cannot be cast to VECTOR[BOOL].")
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[BOOL].")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [Complex32VectorValue].
 *
 * @return [Complex32VectorValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Literal.toComplex32VectorValue(): Complex32VectorValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> throw QueryException.UnsupportedCastException("A value of BOOL cannot be cast to VECTOR[COMPLEX32].")
    CottontailGrpc.Literal.DataCase.INTDATA -> Complex32VectorValue(arrayOf(Complex32Value(this.intData)))
    CottontailGrpc.Literal.DataCase.LONGDATA -> Complex32VectorValue(arrayOf(Complex32Value(this.longData)))
    CottontailGrpc.Literal.DataCase.FLOATDATA -> Complex32VectorValue(arrayOf(Complex32Value(this.floatData)))
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> Complex32VectorValue(arrayOf(Complex32Value(this.doubleData)))
    CottontailGrpc.Literal.DataCase.STRINGDATA -> throw QueryException.UnsupportedCastException("A value of STRING cannot be cast to VECTOR[COMPLEX32].")
    CottontailGrpc.Literal.DataCase.VECTORDATA -> this.vectorData.toComplex32VectorValue()
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> Complex32VectorValue(arrayOf(Complex32Value(FloatValue(this.complex32Data.real), FloatValue(this.complex32Data.imaginary))))
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> Complex32VectorValue(arrayOf(Complex32Value(FloatValue(this.complex64Data.real.toFloat()), FloatValue(this.complex64Data.imaginary.toFloat())))) // cave! precision!
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[COMPLEX32].")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [Complex64VectorValue].
 *
 * @return [Complex64VectorValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Literal.toComplex64VectorValue(): Complex64VectorValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> throw QueryException.UnsupportedCastException("A value of BOOL cannot be cast to VECTOR[COMPLEX64].")
    CottontailGrpc.Literal.DataCase.INTDATA -> Complex64VectorValue(arrayOf(Complex64Value(this.intData)))
    CottontailGrpc.Literal.DataCase.LONGDATA -> Complex64VectorValue(arrayOf(Complex64Value(this.longData)))
    CottontailGrpc.Literal.DataCase.FLOATDATA -> Complex64VectorValue(arrayOf(Complex64Value(this.floatData)))
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> Complex64VectorValue(arrayOf(Complex64Value(this.doubleData)))
    CottontailGrpc.Literal.DataCase.STRINGDATA -> throw QueryException.UnsupportedCastException("A value of STRING cannot be cast to VECTOR[COMPLEX64].")
    CottontailGrpc.Literal.DataCase.VECTORDATA -> this.vectorData.toComplex64VectorValue()
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> Complex64VectorValue(arrayOf(Complex64Value(this.complex32Data.real, this.complex32Data.imaginary)))
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> Complex64VectorValue(arrayOf(Complex64Value(this.complex64Data.real, this.complex64Data.imaginary)))
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[COMPLEX64].")
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
    CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> DoubleVectorValue(this.boolVector.vectorList.map { if (it) 1.0 else 0.0 })
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
    CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> FloatVectorValue(this.boolVector.vectorList.map { if (it) 1.0f else 0.0f })
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
    CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> LongVectorValue(this.boolVector.vectorList.map { if (it) 1L else 0L })
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
    CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> IntVectorValue(this.boolVector.vectorList.map { if (it) 1 else 0 })
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