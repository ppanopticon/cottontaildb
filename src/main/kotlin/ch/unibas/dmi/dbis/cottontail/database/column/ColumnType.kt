package ch.unibas.dmi.dbis.cottontail.database.column

import ch.unibas.dmi.dbis.cottontail.database.serializers.*
import ch.unibas.dmi.dbis.cottontail.model.values.*
import ch.unibas.dmi.dbis.cottontail.model.values.types.Value
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer

import kotlin.reflect.KClass
import kotlin.reflect.full.safeCast

/**
 * Specifies the type of a Cottontail DB [Column]. This construct allows for some degree of type safety in the eye de-/serialization.
 * The column types are stored as strings and mapped to the respective class using [ColumnType.forName].
 *
 * @see Column
 *
 * @author Ralph Gasser
 * @version 1.2
 */
sealed class ColumnType<T : Value> {

    abstract val name: String
    abstract val type: KClass<T>
    abstract val numeric: Boolean

    companion object {
        /** Array of all [ColumnType]s. */
        private val COLUMN_TYPES = arrayOf(
            BooleanColumnType(),
            ByteColumnType(),
            ShortColumnType() ,
            IntColumnType(),
            LongColumnType(),
            FloatColumnType(),
            DoubleColumnType(),
            StringColumnType(),
            Complex32ColumnType(),
            Complex64ColumnType(),
            IntVectorColumnType(),
            LongVectorColumnType(),
            FloatVectorColumnType(),
            DoubleVectorColumnType(),
            BooleanVectorColumnType(),
            Complex32VectorColumnType(),
            Complex64VectorColumnType()
        )

        /**
         * Returns the [ColumnType] for the provided name.
         *
         * @param name For which to lookup the [ColumnType].
         */
        fun forName(name: String): ColumnType<*> = when (name.toUpperCase()) {
            "BOOLEAN" -> COLUMN_TYPES[0]
            "BYTE" -> COLUMN_TYPES[1]
            "SHORT" -> COLUMN_TYPES[2]
            "INTEGER" -> COLUMN_TYPES[3]
            "LONG" -> COLUMN_TYPES[4]
            "FLOAT" -> COLUMN_TYPES[5]
            "DOUBLE" -> COLUMN_TYPES[6]
            "STRING" -> COLUMN_TYPES[7]
            "COMPLEX32" -> COLUMN_TYPES[8]
            "COMPLEX64" -> COLUMN_TYPES[9]
            "INT_VEC" -> COLUMN_TYPES[10]
            "LONG_VEC" -> COLUMN_TYPES[11]
            "FLOAT_VEC" -> COLUMN_TYPES[12]
            "DOUBLE_VEC" -> COLUMN_TYPES[13]
            "BOOL_VEC" -> COLUMN_TYPES[14]
            "COMPLEX32_VEC" -> COLUMN_TYPES[15]
            "COMPLEX64_VEC" -> COLUMN_TYPES[16]
            else -> throw java.lang.IllegalArgumentException("The column type $name does not exists!")
        }

        /**
         * Returns the [ColumnType] for the provided name.
         *
         * @param name For which to lookup the [ColumnType].
         */
        fun forOrdinal(ordinal: Int): ColumnType<*> = COLUMN_TYPES[ordinal]
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
class BooleanColumnType : ColumnType<BooleanValue>() {
    override val name = "BOOLEAN"
    override val numeric = true
    override val type: KClass<BooleanValue> = BooleanValue::class
    override fun serializer(size: Int): Serializer<BooleanValue> = BooleanValueSerializer
}

@Suppress("UNCHECKED_CAST")
class ByteColumnType : ColumnType<ByteValue>() {
    override val name = "BYTE"
    override val numeric = true
    override val type: KClass<ByteValue> = ByteValue::class
    override fun serializer(size: Int): Serializer<ByteValue> = ByteValueSerializer
}

@Suppress("UNCHECKED_CAST")
class ShortColumnType : ColumnType<ShortValue>() {
    override val name = "SHORT"
    override val numeric = true
    override val type: KClass<ShortValue> = ShortValue::class
    override fun serializer(size: Int): Serializer<ShortValue> = ShortValueSerializer
}

@Suppress("UNCHECKED_CAST")
class IntColumnType : ColumnType<IntValue>() {
    override val name = "INTEGER"
    override val numeric = true
    override val type: KClass<IntValue> = IntValue::class
    override fun serializer(size: Int): Serializer<IntValue> = IntValueSerializer
}

@Suppress("UNCHECKED_CAST")
class LongColumnType : ColumnType<LongValue>() {
    override val name = "LONG"
    override val numeric = true
    override val type: KClass<LongValue> = LongValue::class
    override fun serializer(size: Int): Serializer<LongValue> = LongValueSerializer
}

@Suppress("UNCHECKED_CAST")
class FloatColumnType : ColumnType<FloatValue>() {
    override val name = "FLOAT"
    override val numeric = true
    override val type: KClass<FloatValue> = FloatValue::class
    override fun serializer(size: Int): Serializer<FloatValue> = FloatValueSerializer
}

@Suppress("UNCHECKED_CAST")
class DoubleColumnType : ColumnType<DoubleValue>() {
    override val name = "DOUBLE"
    override val numeric = true
    override val type: KClass<DoubleValue> = DoubleValue::class
    override fun serializer(size: Int): Serializer<DoubleValue> = DoubleValueSerializer
}

@Suppress("UNCHECKED_CAST")
class StringColumnType : ColumnType<StringValue>() {
    override val name = "STRING"
    override val numeric = false
    override val type: KClass<StringValue> = StringValue::class
    override fun serializer(size: Int): Serializer<StringValue> {
        require(size < 0) { "Size attribute for a $name type must be > 0 (is $size)." }
        return FixedStringValueSerializer(size)
    }
}

@Suppress("UNCHECKED_CAST")
class Complex32ColumnType : ColumnType<Complex32Value>() {
    override val name = "COMPLEX32"
    override val numeric = true
    override val type: KClass<Complex32Value> = Complex32Value::class
    override fun serializer(size: Int): Serializer<Complex32Value> = Complex32ValueSerializer
}

@Suppress("UNCHECKED_CAST")
class Complex64ColumnType : ColumnType<Complex64Value>() {
    override val name = "COMPLEX64"
    override val numeric = true
    override val type: KClass<Complex64Value> = Complex64Value::class
    override fun serializer(size: Int): Serializer<Complex64Value> = Complex64ValueSerializer
}

@Suppress("UNCHECKED_CAST")
class IntVectorColumnType : ColumnType<IntVectorValue>() {
    override val name = "INT_VEC"
    override val numeric = false
    override val type: KClass<IntVectorValue> = IntVectorValue::class
    override fun serializer(size: Int): Serializer<IntVectorValue> {
        require(size > 0) { "Size attribute for a $name type must be > 0 (is $size)." }
        return FixedIntVectorSerializer(size)
    }
}

@Suppress("UNCHECKED_CAST")
class LongVectorColumnType : ColumnType<LongVectorValue>() {
    override val name = "LONG_VEC"
    override val numeric = false
    override val type: KClass<LongVectorValue> = LongVectorValue::class
    override fun serializer(size: Int): Serializer<LongVectorValue> {
        require(size > 0) { "Size attribute for a $name type must be > 0 (is $size)." }
        return FixedLongVectorSerializer(size)
    }
}

@Suppress("UNCHECKED_CAST")
class FloatVectorColumnType : ColumnType<FloatVectorValue>() {
    override val name = "FLOAT_VEC"
    override val numeric = false
    override val type: KClass<FloatVectorValue> = FloatVectorValue::class
    override fun serializer(size: Int): Serializer<FloatVectorValue> {
        require(size > 0) { "Size attribute for a $name type must be > 0 (is $size)." }
        return FixedFloatVectorSerializer(size)
    }
}

@Suppress("UNCHECKED_CAST")
class DoubleVectorColumnType : ColumnType<DoubleVectorValue>() {
    override val name = "DOUBLE_VEC"
    override val numeric = false
    override val type: KClass<DoubleVectorValue> = DoubleVectorValue::class
    override fun serializer(size: Int): Serializer<DoubleVectorValue> {
        require(size > 0) { "Size attribute for a $name type must be > 0 (is $size)." }
        return FixedDoubleVectorSerializer(size)
    }
}

@Suppress("UNCHECKED_CAST")
class BooleanVectorColumnType : ColumnType<BooleanVectorValue>() {
    override val name = "BOOL_VEC"
    override val numeric = false
    override val type: KClass<BooleanVectorValue> = BooleanVectorValue::class
    override fun serializer(size: Int): Serializer<BooleanVectorValue> {
        require(size > 0) { "Size attribute for a $name type must be > 0 (is $size)." }
        return FixedBooleanVectorSerializer(size)
    }
}

@Suppress("UNCHECKED_CAST")
class Complex32VectorColumnType : ColumnType<Complex32VectorValue>() {
    override val name = "COMPLEX32_VEC"
    override val numeric = false
    override val type: KClass<Complex32VectorValue> = Complex32VectorValue::class
    override fun serializer(size: Int): Serializer<Complex32VectorValue> {
        require(size > 0) { "Size attribute for a $name type must be > 0 (is $size)." }
        return FixedComplex32VectorSerializer(size)
    }
}

@Suppress("UNCHECKED_CAST")
class Complex64VectorColumnType : ColumnType<Complex64VectorValue>() {
    override val name = "COMPLEX64_VEC"
    override val numeric = false
    override val type: KClass<Complex64VectorValue> = Complex64VectorValue::class
    override fun serializer(size: Int): Serializer<Complex64VectorValue> {
        require(size > 0) { "Size attribute for a $name type must be > 0 (is $size)." }
        return FixedComplex64VectorSerializer(size)
    }
}