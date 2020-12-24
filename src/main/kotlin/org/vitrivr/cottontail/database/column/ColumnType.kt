package org.vitrivr.cottontail.database.column

import org.vitrivr.cottontail.database.serializers.*
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer
import kotlin.reflect.KClass
import kotlin.reflect.safeCast

/**
 * Specifies the type of a Cottontail DB [Column]. This construct allows for some degree of type safety in the eye de-/serialization.
 * The column types are stored as strings and mapped to the respective class using [ColumnType.forName].
 *
 * @see Column
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
sealed class ColumnType<T : Value> {

    abstract val name: String
    abstract val ordinal: Int
    abstract val type: KClass<T>
    abstract val numeric: Boolean
    abstract val vector: Boolean
    abstract val size: Int

    companion object {
        /**
         * Returns the [ColumnType] for the provided name.
         *
         * @param name For which to lookup the [ColumnType].
         */
        fun forName(name: String): ColumnType<*> = when (name.toUpperCase()) {
            "BOOLEAN" -> BooleanColumnType
            "BYTE" -> ByteColumnType
            "SHORT" -> ShortColumnType
            "INT",
            "INTEGER" -> IntColumnType
            "LONG" -> LongColumnType
            "FLOAT" -> FloatColumnType
            "DOUBLE" -> DoubleColumnType
            "STRING" -> StringColumnType
            "COMPLEX32" -> Complex32ColumnType
            "COMPLEX64" -> Complex64ColumnType
            "INT_VEC" -> IntVectorColumnType
            "LONG_VEC" -> LongVectorColumnType
            "FLOAT_VEC" -> FloatVectorColumnType
            "DOUBLE_VEC" -> DoubleVectorColumnType
            "BOOL_VEC" -> BooleanVectorColumnType
            "COMPLEX32_VEC" -> Complex32VectorColumnType
            "COMPLEX64_VEC" -> Complex64VectorColumnType
            else -> throw java.lang.IllegalArgumentException("The column type $name does not exists!")
        }

        /**
         * Returns the [ColumnType] for the provided name.
         *
         * @param ordinal for which to lookup the [ColumnType].
         */
        fun forOrdinal(ordinal: Int): ColumnType<*> = when(ordinal) {
            0 -> BooleanColumnType
            1 -> ByteColumnType
            2 -> ShortColumnType
            3 -> IntColumnType
            4 -> LongColumnType
            5 -> FloatColumnType
            6 -> DoubleColumnType
            7 -> StringColumnType
            8 -> Complex32ColumnType
            9 -> Complex64ColumnType
            10 -> IntVectorColumnType
            11 -> LongVectorColumnType
            12 -> FloatVectorColumnType
            13 -> DoubleVectorColumnType
            14 -> BooleanVectorColumnType
            15 -> Complex32VectorColumnType
            16 -> Complex64VectorColumnType
            else -> throw java.lang.IllegalArgumentException("The column type with ordinal $ordinal does not exists!")
        }
    }


    fun cast(value: Value?): T? = this.type.safeCast(value)
    fun compatible(value: Value) = this.type.isInstance(value)

    /**
     * Returns a [Serializer] for this [ColumnType]. Some [ColumnType] require a size attribute
     *
     * @param size The size of the column (e.g. for vectors). Defaults to -1.
     */
    abstract fun serializer(size: Int = -1): Serializer<T>

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as ColumnType<*>
        if (name != other.name) return false
        return true
    }

    override fun hashCode(): Int {
        return name.hashCode()
    }

    override fun toString(): String = this.name
}

@Suppress("UNCHECKED_CAST")
object BooleanColumnType : ColumnType<BooleanValue>() {
    override val name = "BOOLEAN"
    override val ordinal = 0
    override val numeric = true
    override val vector = false
    override val size: Int = Byte.SIZE_BYTES
    override val type: KClass<BooleanValue> = BooleanValue::class
    override fun serializer(size: Int): Serializer<BooleanValue> = BooleanValueSerializer
}

@Suppress("UNCHECKED_CAST")
object ByteColumnType : ColumnType<ByteValue>() {
    override val name = "BYTE"
    override val ordinal = 1
    override val numeric = true
    override val vector = false
    override val size: Int = Byte.SIZE_BYTES
    override val type: KClass<ByteValue> = ByteValue::class
    override fun serializer(size: Int): Serializer<ByteValue> = ByteValueSerializer
}

@Suppress("UNCHECKED_CAST")
object ShortColumnType : ColumnType<ShortValue>() {
    override val name = "SHORT"
    override val ordinal = 2
    override val numeric = true
    override val vector = false
    override val size: Int = Short.SIZE_BYTES
    override val type: KClass<ShortValue> = ShortValue::class
    override fun serializer(size: Int): Serializer<ShortValue> = ShortValueSerializer
}

@Suppress("UNCHECKED_CAST")
object IntColumnType : ColumnType<IntValue>() {
    override val name = "INTEGER"
    override val ordinal = 3
    override val numeric = true
    override val vector = false
    override val size: Int = Int.SIZE_BYTES
    override val type: KClass<IntValue> = IntValue::class
    override fun serializer(size: Int): Serializer<IntValue> = IntValueSerializer
}

@Suppress("UNCHECKED_CAST")
object LongColumnType : ColumnType<LongValue>() {
    override val name = "LONG"
    override val ordinal = 4
    override val numeric = true
    override val vector = false
    override val size: Int = Long.SIZE_BYTES
    override val type: KClass<LongValue> = LongValue::class
    override fun serializer(size: Int): Serializer<LongValue> = LongValueSerializer
}

@Suppress("UNCHECKED_CAST")
object FloatColumnType : ColumnType<FloatValue>() {
    override val name = "FLOAT"
    override val ordinal = 5
    override val numeric = true
    override val vector = false
    override val size: Int = Int.SIZE_BYTES
    override val type: KClass<FloatValue> = FloatValue::class
    override fun serializer(size: Int): Serializer<FloatValue> = FloatValueSerializer
}

@Suppress("UNCHECKED_CAST")
object DoubleColumnType : ColumnType<DoubleValue>() {
    override val name = "DOUBLE"
    override val ordinal = 6
    override val numeric = true
    override val vector = false
    override val size: Int = Long.SIZE_BYTES
    override val type: KClass<DoubleValue> = DoubleValue::class
    override fun serializer(size: Int): Serializer<DoubleValue> = DoubleValueSerializer
}

@Suppress("UNCHECKED_CAST")
object StringColumnType : ColumnType<StringValue>() {
    override val name = "STRING"
    override val ordinal = 7
    override val numeric = false
    override val vector = false
    override val size: Int = Char.SIZE_BYTES
    override val type: KClass<StringValue> = StringValue::class
    override fun serializer(size: Int): Serializer<StringValue> {
        //require(size > 0) { "Size attribute for a $name type must be > 0 (is $size)." }
        return FixedStringValueSerializer(size)
    }
}

@Suppress("UNCHECKED_CAST")
object Complex32ColumnType : ColumnType<Complex32Value>() {
    override val name = "COMPLEX32"
    override val ordinal = 8
    override val numeric = true
    override val vector = false
    override val size: Int = 2 * Int.SIZE_BYTES
    override val type: KClass<Complex32Value> = Complex32Value::class
    override fun serializer(size: Int): Serializer<Complex32Value> = Complex32ValueSerializer
}

@Suppress("UNCHECKED_CAST")
object Complex64ColumnType : ColumnType<Complex64Value>() {
    override val name = "COMPLEX64"
    override val ordinal = 9
    override val numeric = true
    override val vector = false
    override val size: Int = 2 * Long.SIZE_BYTES
    override val type: KClass<Complex64Value> = Complex64Value::class
    override fun serializer(size: Int): Serializer<Complex64Value> = Complex64ValueSerializer
}

@Suppress("UNCHECKED_CAST")
object IntVectorColumnType : ColumnType<IntVectorValue>() {
    override val name = "INT_VEC"
    override val ordinal = 10
    override val numeric = false
    override val vector = true
    override val size: Int = Int.SIZE_BYTES
    override val type: KClass<IntVectorValue> = IntVectorValue::class
    override fun serializer(size: Int): Serializer<IntVectorValue> {
        require(size > 0) { "Size attribute for a $name type must be > 0 (is $size)." }
        return FixedIntVectorSerializer(size)
    }
}

@Suppress("UNCHECKED_CAST")
object LongVectorColumnType : ColumnType<LongVectorValue>() {
    override val name = "LONG_VEC"
    override val ordinal = 11
    override val numeric = false
    override val vector = true
    override val size: Int = Long.SIZE_BYTES
    override val type: KClass<LongVectorValue> = LongVectorValue::class
    override fun serializer(size: Int): Serializer<LongVectorValue> {
        require(size > 0) { "Size attribute for a $name type must be > 0 (is $size)." }
        return FixedLongVectorSerializer(size)
    }
}

@Suppress("UNCHECKED_CAST")
object FloatVectorColumnType : ColumnType<FloatVectorValue>() {
    override val name = "FLOAT_VEC"
    override val ordinal = 12
    override val numeric = false
    override val vector = true
    override val size: Int = Int.SIZE_BYTES
    override val type: KClass<FloatVectorValue> = FloatVectorValue::class
    override fun serializer(size: Int): Serializer<FloatVectorValue> {
        require(size > 0) { "Size attribute for a $name type must be > 0 (is $size)." }
        return FixedFloatVectorSerializer(size)
    }
}

@Suppress("UNCHECKED_CAST")
object DoubleVectorColumnType : ColumnType<DoubleVectorValue>() {
    override val name = "DOUBLE_VEC"
    override val ordinal = 13
    override val numeric = false
    override val vector = true
    override val size: Int = Long.SIZE_BYTES
    override val type: KClass<DoubleVectorValue> = DoubleVectorValue::class
    override fun serializer(size: Int): Serializer<DoubleVectorValue> {
        require(size > 0) { "Size attribute for a $name type must be > 0 (is $size)." }
        return FixedDoubleVectorSerializer(size)
    }
}

@Suppress("UNCHECKED_CAST")
object BooleanVectorColumnType : ColumnType<BooleanVectorValue>() {
    override val name = "BOOL_VEC"
    override val ordinal = 14
    override val numeric = false
    override val vector = true
    override val size: Int = Byte.SIZE_BYTES
    override val type: KClass<BooleanVectorValue> = BooleanVectorValue::class
    override fun serializer(size: Int): Serializer<BooleanVectorValue> {
        require(size > 0) { "Size attribute for a $name type must be > 0 (is $size)." }
        return FixedBooleanVectorSerializer(size)
    }
}

@Suppress("UNCHECKED_CAST")
object Complex32VectorColumnType : ColumnType<Complex32VectorValue>() {
    override val name = "COMPLEX32_VEC"
    override val ordinal = 15
    override val numeric = false
    override val vector = true
    override val size: Int = 2 * Int.SIZE_BYTES
    override val type: KClass<Complex32VectorValue> = Complex32VectorValue::class
    override fun serializer(size: Int): Serializer<Complex32VectorValue> {
        require(size > 0) { "Size attribute for a $name type must be > 0 (is $size)." }
        return FixedComplex32VectorSerializer(size)
    }
}

@Suppress("UNCHECKED_CAST")
object Complex64VectorColumnType : ColumnType<Complex64VectorValue>() {
    override val name = "COMPLEX64_VEC"
    override val ordinal = 16
    override val numeric = false
    override val vector = true
    override val size: Int = 2 * Long.SIZE_BYTES
    override val type: KClass<Complex64VectorValue> = Complex64VectorValue::class
    override fun serializer(size: Int): Serializer<Complex64VectorValue> {
        require(size > 0) { "Size attribute for a $name type must be > 0 (is $size)." }
        return FixedComplex64VectorSerializer(size)
    }
}