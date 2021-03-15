package org.vitrivr.cottontail.model.basics

import org.vitrivr.cottontail.database.statistics.columns.*
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.serializers.*

/**
 * Specifies the [Type] of a Cottontail DB column or value. This construct is a centerpiece of
 * Cottontail DB's type system and allows for some  degree of type safety in the eye de-/serialization,
 * conversion and casting.
 *
 * Upon serialization, [Type]s can be stored as strings and mapped to the respective class using [Type.forName].
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
sealed class Type<T : Value> {

    companion object {
        /** Constant for unknown logical size. */
        const val LOGICAL_SIZE_UNKNOWN = -1

        /**
         * Returns the [Type] for the provided name.
         *
         * @param name For which to lookup the [Type].
         * @param logicalSize The logical size of this [Type] (only for vector types).
         */
        fun forName(name: kotlin.String, logicalSize: kotlin.Int): Type<*> = when (name.toUpperCase()) {
            "BOOLEAN" -> Boolean
            "BYTE" -> Byte
            "SHORT" -> Short
            "INT",
            "INTEGER" -> Int
            "LONG" -> Long
            "DATE" -> Date
            "FLOAT" -> Float
            "DOUBLE" -> Double
            "STRING" -> String
            "COMPLEX32" -> Complex32
            "COMPLEX64" -> Complex64
            "INT_VEC" -> IntVector(logicalSize)
            "LONG_VEC" -> LongVector(logicalSize)
            "FLOAT_VEC" -> FloatVector(logicalSize)
            "DOUBLE_VEC" -> DoubleVector(logicalSize)
            "BOOL_VEC" -> BooleanVector(logicalSize)
            "COMPLEX32_VEC" -> Complex32Vector(logicalSize)
            "COMPLEX64_VEC" -> Complex64Vector(logicalSize)
            else -> throw java.lang.IllegalArgumentException("The column type $name does not exists!")
        }

        /**
         * Returns the [Type] for the provided ordinal.
         *
         * @param ordinal For which to lookup the [Type].
         * @param logicalSize The logical size of this [Type] (only for vector types).
         */
        fun forOrdinal(ordinal: kotlin.Int, logicalSize: kotlin.Int) = when (ordinal) {
            0 -> Boolean
            1 -> Byte
            2 -> Short
            3 -> Int
            4 -> Long
            5 -> Date
            6 -> Float
            7 -> Double
            8 -> String
            9 -> Complex32
            10 -> Complex64
            11 -> IntVector(logicalSize)
            12 -> LongVector(logicalSize)
            13 -> FloatVector(logicalSize)
            14 -> DoubleVector(logicalSize)
            15 -> BooleanVector(logicalSize)
            16 -> Complex32Vector(logicalSize)
            17 -> Complex64Vector(logicalSize)
            else -> throw java.lang.IllegalArgumentException("The column type for ordinal $ordinal does not exists!")
        }
    }

    abstract val name: kotlin.String
    abstract val numeric: kotlin.Boolean
    abstract val complex: kotlin.Boolean
    abstract val vector: kotlin.Boolean
    abstract val logicalSize: kotlin.Int
    abstract val physicalSize: kotlin.Int
    abstract val ordinal: kotlin.Int

    fun compatible(value: Value) = this == value.type

    /** Returns the default value for this [Type]. */
    abstract fun defaultValue(): T

    /** Returns a [ValueSerializerFactory] for this [Type]. */
    abstract fun serializerFactory(): ValueSerializerFactory<T>

    /** Creates and returns a [ValueStatistics] object for this [Type]. */
    abstract fun statistics(): ValueStatistics<T>

    override fun equals(other: Any?): kotlin.Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as Type<*>
        if (name != other.name) return false
        return true
    }

    override fun hashCode(): kotlin.Int {
        return this.name.hashCode()
    }

    override fun toString(): kotlin.String = this.name

    @Suppress("UNCHECKED_CAST")
    object Boolean : Type<BooleanValue>() {
        override val name = "BOOLEAN"
        override val ordinal: kotlin.Int = 0
        override val numeric = true
        override val complex = false
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = kotlin.Byte.SIZE_BYTES
        override fun defaultValue(): BooleanValue = BooleanValue.FALSE
        override fun serializerFactory(): ValueSerializerFactory<BooleanValue> = BooleanValueSerializerFactory
        override fun statistics(): ValueStatistics<BooleanValue> = BooleanValueStatistics()
    }

    @Suppress("UNCHECKED_CAST")
    object Byte : Type<ByteValue>() {
        override val name = "BYTE"
        override val ordinal: kotlin.Int = 1
        override val numeric = true
        override val complex = false
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = kotlin.Byte.SIZE_BYTES
        override fun defaultValue(): ByteValue = ByteValue.ZERO
        override fun serializerFactory(): ValueSerializerFactory<ByteValue> = ByteValueSerializerFactory
        override fun statistics(): ValueStatistics<ByteValue> = ByteValueStatistics()
    }

    @Suppress("UNCHECKED_CAST")
    object Short : Type<ShortValue>() {
        override val name = "SHORT"
        override val ordinal: kotlin.Int = 2
        override val numeric = true
        override val complex = false
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = kotlin.Short.SIZE_BYTES
        override fun defaultValue(): ShortValue = ShortValue.ZERO
        override fun serializerFactory(): ValueSerializerFactory<ShortValue> = ShortValueSerializerFactory
        override fun statistics(): ValueStatistics<ShortValue> = ShortValueStatistics()
    }

    @Suppress("UNCHECKED_CAST")
    object Int : Type<IntValue>() {
        override val name = "INTEGER"
        override val ordinal: kotlin.Int = 3
        override val numeric = true
        override val complex = false
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = kotlin.Int.SIZE_BYTES
        override fun defaultValue(): IntValue = IntValue.ZERO
        override fun serializerFactory(): ValueSerializerFactory<IntValue> = IntValueSerializerFactory
        override fun statistics(): ValueStatistics<IntValue> = IntValueStatistics()
    }

    @Suppress("UNCHECKED_CAST")
    object Long : Type<LongValue>() {
        override val name = "LONG"
        override val ordinal: kotlin.Int = 4
        override val numeric = true
        override val complex = false
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = kotlin.Long.SIZE_BYTES
        override fun defaultValue(): LongValue = LongValue.ZERO
        override fun serializerFactory(): ValueSerializerFactory<LongValue> = LongValueSerializerFactory
        override fun statistics(): ValueStatistics<LongValue> = LongValueStatistics()
    }

    @Suppress("UNCHECKED_CAST")
    object Date : Type<DateValue>() {
        override val name = "DATE"
        override val ordinal: kotlin.Int = 5
        override val numeric = false
        override val complex = false
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = kotlin.Long.SIZE_BYTES
        override fun defaultValue(): DateValue = DateValue(System.currentTimeMillis())
        override fun serializerFactory(): ValueSerializerFactory<DateValue> = DateValueSerializerFactory
        override fun statistics(): ValueStatistics<DateValue> = DateValueStatistics()
    }

    @Suppress("UNCHECKED_CAST")
    object Float : Type<FloatValue>() {
        override val name = "FLOAT"
        override val ordinal: kotlin.Int = 6
        override val numeric = true
        override val complex = false
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = kotlin.Int.SIZE_BYTES
        override fun defaultValue(): FloatValue = FloatValue.ZERO
        override fun serializerFactory(): ValueSerializerFactory<FloatValue> = FloatValueSerializerFactory
        override fun statistics(): ValueStatistics<FloatValue> = FloatValueStatistics()
    }

    @Suppress("UNCHECKED_CAST")
    object Double : Type<DoubleValue>() {
        override val name = "DOUBLE"
        override val ordinal: kotlin.Int = 7
        override val numeric = true
        override val complex = false
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = kotlin.Long.SIZE_BYTES
        override fun defaultValue(): DoubleValue = DoubleValue.ZERO
        override fun serializerFactory(): ValueSerializerFactory<DoubleValue> = DoubleValueSerializerFactory
        override fun statistics(): ValueStatistics<DoubleValue> = DoubleValueStatistics()
    }

    @Suppress("UNCHECKED_CAST")
    object String : Type<StringValue>() {
        override val name = "STRING"
        override val ordinal: kotlin.Int = 8
        override val numeric = false
        override val complex = false
        override val vector = false
        override val logicalSize = LOGICAL_SIZE_UNKNOWN
        override val physicalSize = LOGICAL_SIZE_UNKNOWN * Char.SIZE_BYTES
        override fun defaultValue(): StringValue = StringValue.EMPTY
        override fun serializerFactory(): ValueSerializerFactory<StringValue> = StringValueSerializerFactory
        override fun statistics(): ValueStatistics<StringValue> = StringValueStatistics()
    }

    @Suppress("UNCHECKED_CAST")
    object Complex32 : Type<Complex32Value>() {
        override val name = "COMPLEX32"
        override val ordinal: kotlin.Int = 9
        override val numeric = true
        override val complex = true
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = 2 * kotlin.Int.SIZE_BYTES
        override fun defaultValue(): Complex32Value = Complex32Value.ZERO
        override fun serializerFactory(): ValueSerializerFactory<Complex32Value> = Complex32ValueSerializerFactory
        override fun statistics(): ValueStatistics<Complex32Value> = ValueStatistics(this)
    }

    @Suppress("UNCHECKED_CAST")
    object Complex64 : Type<Complex64Value>() {
        override val name = "COMPLEX64"
        override val ordinal: kotlin.Int = 10
        override val numeric = true
        override val complex = true
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = 2 * kotlin.Long.SIZE_BYTES
        override fun defaultValue(): Complex64Value = Complex64Value.ZERO
        override fun serializerFactory(): ValueSerializerFactory<Complex64Value> = Complex64ValueSerializerFactory
        override fun statistics(): ValueStatistics<Complex64Value> = ValueStatistics(this)
    }

    @Suppress("UNCHECKED_CAST")
    class IntVector(override val logicalSize: kotlin.Int) : Type<IntVectorValue>() {
        override val name = "INT_VEC"
        override val ordinal: kotlin.Int = 11
        override val numeric = false
        override val complex = false
        override val vector = true
        override val physicalSize = this.logicalSize * kotlin.Int.SIZE_BYTES
        override fun defaultValue(): IntVectorValue = IntVectorValue.zero(this.logicalSize)
        override fun serializerFactory(): ValueSerializerFactory<IntVectorValue> = IntVectorValueSerializerFactory
        override fun statistics(): ValueStatistics<IntVectorValue> = IntVectorValueStatistics(this)
    }

    @Suppress("UNCHECKED_CAST")
    class LongVector(override val logicalSize: kotlin.Int) : Type<LongVectorValue>() {
        override val name = "LONG_VEC"
        override val ordinal: kotlin.Int = 12
        override val numeric = false
        override val complex = false
        override val vector = true
        override val physicalSize = this.logicalSize * kotlin.Long.SIZE_BYTES
        override fun defaultValue(): LongVectorValue = LongVectorValue.zero(this.logicalSize)
        override fun serializerFactory(): ValueSerializerFactory<LongVectorValue> = LongVectorValueSerializerFactory
        override fun statistics(): ValueStatistics<LongVectorValue> = LongVectorValueStatistics(this)
    }

    @Suppress("UNCHECKED_CAST")
    class FloatVector(override val logicalSize: kotlin.Int) : Type<FloatVectorValue>() {
        override val name = "FLOAT_VEC"
        override val ordinal: kotlin.Int = 13
        override val numeric = false
        override val complex = false
        override val vector = true
        override val physicalSize = this.logicalSize * kotlin.Int.SIZE_BYTES
        override fun defaultValue(): FloatVectorValue = FloatVectorValue.zero(this.logicalSize)
        override fun serializerFactory(): ValueSerializerFactory<FloatVectorValue> = FloatVectorValueSerializerFactory
        override fun statistics(): ValueStatistics<FloatVectorValue> = FloatVectorValueStatistics(this)
    }

    @Suppress("UNCHECKED_CAST")
    class DoubleVector(override val logicalSize: kotlin.Int) : Type<DoubleVectorValue>() {
        override val name = "DOUBLE_VEC"
        override val ordinal: kotlin.Int = 14
        override val numeric = false
        override val complex = false
        override val vector = true
        override val physicalSize = this.logicalSize * kotlin.Long.SIZE_BYTES
        override fun defaultValue(): DoubleVectorValue = DoubleVectorValue.zero(this.logicalSize)
        override fun serializerFactory(): ValueSerializerFactory<DoubleVectorValue> = DoubleVectorValueSerializerFactory
        override fun statistics(): ValueStatistics<DoubleVectorValue> = DoubleVectorValueStatistics(this)
    }

    @Suppress("UNCHECKED_CAST")
    class BooleanVector(override val logicalSize: kotlin.Int) : Type<BooleanVectorValue>() {
        override val name = "BOOL_VEC"
        override val ordinal: kotlin.Int = 15
        override val numeric = false
        override val complex = false
        override val vector = true
        override val physicalSize = this.logicalSize * kotlin.Byte.SIZE_BYTES
        override fun defaultValue(): BooleanVectorValue = BooleanVectorValue.zero(this.logicalSize)
        override fun serializerFactory(): ValueSerializerFactory<BooleanVectorValue> = BooleanVectorValueSerializerFactory
        override fun statistics(): ValueStatistics<BooleanVectorValue> = BooleanVectorValueStatistics(this)
    }

    @Suppress("UNCHECKED_CAST")
    class Complex32Vector(override val logicalSize: kotlin.Int) : Type<Complex32VectorValue>() {
        override val name = "COMPLEX32_VEC"
        override val ordinal: kotlin.Int = 16
        override val numeric = false
        override val complex = true
        override val vector = true
        override val physicalSize = this.logicalSize * 2 * kotlin.Int.SIZE_BYTES
        override fun defaultValue(): Complex32VectorValue = Complex32VectorValue.zero(this.logicalSize)
        override fun serializerFactory(): ValueSerializerFactory<Complex32VectorValue> = Complex32VectorValueSerializerFactory
        override fun statistics(): ValueStatistics<Complex32VectorValue> = ValueStatistics(this)
    }

    @Suppress("UNCHECKED_CAST")
    class Complex64Vector(override val logicalSize: kotlin.Int) : Type<Complex64VectorValue>() {
        override val name = "COMPLEX64_VEC"
        override val ordinal: kotlin.Int = 17
        override val numeric = false
        override val complex = true
        override val vector = true
        override val physicalSize = this.logicalSize * 2 * kotlin.Long.SIZE_BYTES
        override fun defaultValue(): Complex64VectorValue = Complex64VectorValue.zero(this.logicalSize)
        override fun serializerFactory(): ValueSerializerFactory<Complex64VectorValue> = Complex64VectorValueSerializerFactory
        override fun statistics(): ValueStatistics<Complex64VectorValue> = ValueStatistics(this)
    }
}