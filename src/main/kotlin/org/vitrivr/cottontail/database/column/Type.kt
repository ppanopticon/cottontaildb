package org.vitrivr.cottontail.database.column

import org.mapdb.Serializer
import org.vitrivr.cottontail.database.serializers.*
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value

/**
 * Specifies the [Type] of a Cottontail DB [Column] or [Value]. This construct is a centerpiece of
 * Cottontail DB's type system and allows for some  degree of type safety in the eye de-/serialization,
 * conversion and casting.
 *
 * Upon serialization, [Type]s can be stored as strings and mapped to the respective class using [Type.forName].
 *
 * @see Column
 *
 * @author Ralph Gasser
 * @version 1.2.0
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
        fun forName(name: kotlin.String, logicalSize: kotlin.Int): Type<*> =
            when (name.toUpperCase()) {
                "BOOLEAN" -> Boolean
                "BYTE" -> Byte
                "SHORT" -> Short
                "INT",
                "INTEGER" -> Int
                "LONG" -> Long
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
    }

    abstract val name: kotlin.String
    abstract val numeric: kotlin.Boolean
    abstract val complex: kotlin.Boolean
    abstract val vector: kotlin.Boolean
    abstract val logicalSize: kotlin.Int
    abstract val physicalSize: kotlin.Int

    fun compatible(value: Value) = this == value.type

    /** Returns the default value for this [Type]. */
    abstract fun defaultValue(): T

    /** Creates and returns a [Serializer] for this [Type]. */
    abstract fun serializer(): Serializer<T>

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
        override val numeric = true
        override val complex = false
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = kotlin.Byte.SIZE_BYTES
        override fun defaultValue(): BooleanValue = BooleanValue.FALSE
        override fun serializer(): Serializer<BooleanValue> = BooleanValueSerializer
    }

    @Suppress("UNCHECKED_CAST")
    object Byte : Type<ByteValue>() {
        override val name = "BYTE"
        override val numeric = true
        override val complex = false
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = kotlin.Byte.SIZE_BYTES
        override fun defaultValue(): ByteValue = ByteValue.ZERO
        override fun serializer(): Serializer<ByteValue> = ByteValueSerializer
    }

    @Suppress("UNCHECKED_CAST")
    object Short : Type<ShortValue>() {
        override val name = "SHORT"
        override val numeric = true
        override val complex = false
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = kotlin.Short.SIZE_BYTES
        override fun defaultValue(): ShortValue = ShortValue.ZERO
        override fun serializer(): Serializer<ShortValue> = ShortValueSerializer
    }

    @Suppress("UNCHECKED_CAST")
    object Int : Type<IntValue>() {
        override val name = "INTEGER"
        override val numeric = true
        override val complex = false
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = kotlin.Int.SIZE_BYTES
        override fun defaultValue(): IntValue = IntValue.ZERO
        override fun serializer(): Serializer<IntValue> = IntValueSerializer
    }

    @Suppress("UNCHECKED_CAST")
    object Long : Type<LongValue>() {
        override val name = "LONG"
        override val numeric = true
        override val complex = false
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = kotlin.Long.SIZE_BYTES
        override fun defaultValue(): LongValue = LongValue.ZERO
        override fun serializer(): Serializer<LongValue> = LongValueSerializer
    }

    @Suppress("UNCHECKED_CAST")
    object Date : Type<DateValue>() {
        override val name = "DATE"
        override val numeric = false
        override val complex = false
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = kotlin.Long.SIZE_BYTES
        override fun defaultValue(): DateValue = DateValue(System.currentTimeMillis())
        override fun serializer(): Serializer<DateValue> = DateValueSerializer
    }

    @Suppress("UNCHECKED_CAST")
    object Float : Type<FloatValue>() {
        override val name = "FLOAT"
        override val numeric = true
        override val complex = false
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = kotlin.Int.SIZE_BYTES
        override fun defaultValue(): FloatValue = FloatValue.ZERO
        override fun serializer(): Serializer<FloatValue> = FloatValueSerializer
    }

    @Suppress("UNCHECKED_CAST")
    object Double : Type<DoubleValue>() {
        override val name = "DOUBLE"
        override val numeric = true
        override val complex = false
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = kotlin.Long.SIZE_BYTES
        override fun defaultValue(): DoubleValue = DoubleValue.ZERO
        override fun serializer(): Serializer<DoubleValue> = DoubleValueSerializer
    }

    @Suppress("UNCHECKED_CAST")
    object String : Type<StringValue>() {
        override val name = "STRING"
        override val numeric = false
        override val complex = false
        override val vector = false
        override val logicalSize = LOGICAL_SIZE_UNKNOWN
        override val physicalSize = LOGICAL_SIZE_UNKNOWN * Char.SIZE_BYTES
        override fun defaultValue(): StringValue = StringValue.EMPTY
        override fun serializer(): Serializer<StringValue> = StringValueSerializer
    }

    @Suppress("UNCHECKED_CAST")
    object Complex32 : Type<Complex32Value>() {
        override val name = "COMPLEX32"
        override val numeric = true
        override val complex = true
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = 2 * kotlin.Int.SIZE_BYTES
        override fun defaultValue(): Complex32Value = Complex32Value.ZERO
        override fun serializer(): Serializer<Complex32Value> = Complex32ValueSerializer
    }

    @Suppress("UNCHECKED_CAST")
    object Complex64 : Type<Complex64Value>() {
        override val name = "COMPLEX64"
        override val numeric = true
        override val complex = true
        override val vector = false
        override val logicalSize = 1
        override val physicalSize = 2 * kotlin.Long.SIZE_BYTES
        override fun defaultValue(): Complex64Value = Complex64Value.ZERO
        override fun serializer(): Serializer<Complex64Value> = Complex64ValueSerializer
    }

    @Suppress("UNCHECKED_CAST")
    class IntVector(override val logicalSize: kotlin.Int) : Type<IntVectorValue>() {
        override val name = "INT_VEC"
        override val numeric = false
        override val complex = false
        override val vector = true
        override val physicalSize = this.logicalSize * kotlin.Int.SIZE_BYTES
        override fun defaultValue(): IntVectorValue = IntVectorValue.zero(this.logicalSize)
        override fun serializer(): Serializer<IntVectorValue> =
            FixedIntVectorSerializer(this.logicalSize)
    }

    @Suppress("UNCHECKED_CAST")
    class LongVector(override val logicalSize: kotlin.Int) : Type<LongVectorValue>() {
        override val name = "LONG_VEC"
        override val numeric = false
        override val complex = false
        override val vector = true
        override val physicalSize = this.logicalSize * kotlin.Long.SIZE_BYTES
        override fun defaultValue(): LongVectorValue = LongVectorValue.zero(this.logicalSize)
        override fun serializer(): Serializer<LongVectorValue> =
            FixedLongVectorSerializer(this.logicalSize)
    }

    @Suppress("UNCHECKED_CAST")
    class FloatVector(override val logicalSize: kotlin.Int) : Type<FloatVectorValue>() {
        override val name = "FLOAT_VEC"
        override val numeric = false
        override val complex = false
        override val vector = true
        override val physicalSize = this.logicalSize * kotlin.Int.SIZE_BYTES
        override fun defaultValue(): FloatVectorValue = FloatVectorValue.zero(this.logicalSize)
        override fun serializer(): Serializer<FloatVectorValue> =
            FixedFloatVectorSerializer(this.logicalSize)
    }

    @Suppress("UNCHECKED_CAST")
    class DoubleVector(override val logicalSize: kotlin.Int) : Type<DoubleVectorValue>() {
        override val name = "DOUBLE_VEC"
        override val numeric = false
        override val complex = false
        override val vector = true
        override val physicalSize = this.logicalSize * kotlin.Long.SIZE_BYTES
        override fun defaultValue(): DoubleVectorValue = DoubleVectorValue.zero(this.logicalSize)
        override fun serializer(): Serializer<DoubleVectorValue> =
            FixedDoubleVectorSerializer(this.logicalSize)
    }

    @Suppress("UNCHECKED_CAST")
    class BooleanVector(override val logicalSize: kotlin.Int) : Type<BooleanVectorValue>() {
        override val name = "BOOL_VEC"
        override val numeric = false
        override val complex = false
        override val vector = true
        override val physicalSize = this.logicalSize * kotlin.Byte.SIZE_BYTES
        override fun defaultValue(): BooleanVectorValue = BooleanVectorValue.zero(this.logicalSize)
        override fun serializer(): Serializer<BooleanVectorValue> =
            FixedBooleanVectorSerializer(this.logicalSize)
    }

    @Suppress("UNCHECKED_CAST")
    class Complex32Vector(override val logicalSize: kotlin.Int) : Type<Complex32VectorValue>() {
        override val name = "COMPLEX32_VEC"
        override val numeric = false
        override val complex = true
        override val vector = true
        override val physicalSize = this.logicalSize * 2 * kotlin.Int.SIZE_BYTES
        override fun defaultValue(): Complex32VectorValue =
            Complex32VectorValue.zero(this.logicalSize)

        override fun serializer(): Serializer<Complex32VectorValue> =
            FixedComplex32VectorSerializer(this.logicalSize)
    }

    @Suppress("UNCHECKED_CAST")
    class Complex64Vector(override val logicalSize: kotlin.Int) : Type<Complex64VectorValue>() {
        override val name = "COMPLEX64_VEC"
        override val numeric = false
        override val complex = true
        override val vector = true
        override val physicalSize = this.logicalSize * 2 * kotlin.Long.SIZE_BYTES
        override fun defaultValue(): Complex64VectorValue =
            Complex64VectorValue.zero(this.logicalSize)

        override fun serializer(): Serializer<Complex64VectorValue> =
            FixedComplex64VectorSerializer(this.logicalSize)
    }
}