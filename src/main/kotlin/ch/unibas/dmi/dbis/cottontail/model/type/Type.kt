package ch.unibas.dmi.dbis.cottontail.model.type

import ch.unibas.dmi.dbis.cottontail.database.serializers.*
import ch.unibas.dmi.dbis.cottontail.model.values.*

import org.mapdb.Serializer

import kotlin.reflect.KClass
import kotlin.reflect.full.safeCast

/**
 * Specifies the type of a Cottontail DB [Column] or a [Value]. This construct allows for some degree of type safety in the eye de-/serialization.
 * The column types can be stored as strings and mapped to the respective class using [TypeFactory.typeForName()].
 *
 * @see Column
 * @see TypeFactory
 *
 * @author Ralph Gasser
 * @version 1.1
 */
sealed class Type<T : Any> {
    abstract val name : String
    abstract val kotlinType: KClass<out Value<T>>
    abstract val numeric: Boolean

    fun cast(value: Value<*>?) : Value<T>? = this.kotlinType.safeCast(value)
    fun compatible(value: Value<*>) = this.kotlinType.isInstance(value)

    /**
     * Returns a [Serializer] for this [Type]. Some [Type] require a size attribute
     *
     * @param size The size of the column (e.g. for vectors). Defaults to -1.
     */
    abstract fun serializer(size: Int = -1): Serializer<Value<T>>


    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as Type<*>
        if (name != other.name) return false
        return true
    }

    override fun hashCode(): Int {
        return name.hashCode()
    }

    override fun toString(): String = this.name
}

@Suppress("UNCHECKED_CAST")
object BooleanType : Type<Boolean>() {
    override val name = "BOOLEAN"
    override val numeric = true
    override val kotlinType: KClass<BooleanValue> = BooleanValue::class
    override fun serializer(size: Int): Serializer<Value<Boolean>> = BooleanValueSerializer as Serializer<Value<Boolean>>
}

@Suppress("UNCHECKED_CAST")
object ByteType : Type<Byte>() {
    override val name = "BYTE"
    override val numeric = true
    override val kotlinType: KClass<ByteValue> = ByteValue::class
    override fun serializer(size: Int): Serializer<Value<Byte>> = ByteValueSerializer as Serializer<Value<Byte>>
}

@Suppress("UNCHECKED_CAST")
object ShortType : Type<Short>() {
    override val name = "SHORT"
    override val numeric = true
    override val kotlinType: KClass<ShortValue> = ShortValue::class
    override fun serializer(size: Int): Serializer<Value<Short>> = ShortValueSerializer  as Serializer<Value<Short>>
}

@Suppress("UNCHECKED_CAST")
object IntType : Type<Int>() {
    override val name = "INTEGER"
    override val numeric = true
    override val kotlinType: KClass<IntValue> = IntValue::class
    override fun serializer(size: Int): Serializer<Value<Int>> = IntValueSerializer  as Serializer<Value<Int>>
}

@Suppress("UNCHECKED_CAST")
object LongType : Type<Long>() {
    override val name = "LONG"
    override val numeric = true
    override val kotlinType: KClass<LongValue> = LongValue::class
    override fun serializer(size: Int): Serializer<Value<Long>> = LongValueSerializer  as Serializer<Value<Long>>
}

@Suppress("UNCHECKED_CAST")
object FloatType : Type<Float>() {
    override val name = "FLOAT"
    override val numeric = true
    override val kotlinType: KClass<FloatValue> = FloatValue::class
    override fun serializer(size: Int): Serializer<Value<Float>> = FloatValueSerializer  as Serializer<Value<Float>>
}

@Suppress("UNCHECKED_CAST")
object DoubleType : Type<Double>() {
    override val name = "DOUBLE"
    override val numeric = true
    override val kotlinType: KClass<DoubleValue> = DoubleValue::class
    override fun serializer(size: Int): Serializer<Value<Double>> = DoubleValueSerializer as Serializer<Value<Double>>
}

@Suppress("UNCHECKED_CAST")
object StringType : Type<String>() {
    override val name = "STRING"
    override val numeric = false
    override val kotlinType: KClass<StringValue> = StringValue::class
    override fun serializer(size: Int): Serializer<Value<String>> = StringValueSerializer as Serializer<Value<String>>
}

@Suppress("UNCHECKED_CAST")
object IntArrayType : Type<IntArray>() {
    override val name = "INT_VEC"
    override val numeric = false
    override val kotlinType: KClass<IntArrayValue> = IntArrayValue::class
    override fun serializer(size: Int): Serializer<Value<IntArray>> {
        if (size <= 0) throw IllegalArgumentException("Size attribute for a $name type must be > 0 (is $size).")
        return FixedIntArraySerializer(size) as Serializer<Value<IntArray>>
    }
}

@Suppress("UNCHECKED_CAST")
object LongArrayType : Type<LongArray>() {
    override val name = "LONG_VEC"
    override val numeric = false
    override val kotlinType: KClass<LongArrayValue> = LongArrayValue::class
    override fun serializer(size: Int): Serializer<Value<LongArray>> {
        if (size <= 0) throw IllegalArgumentException("Size attribute for a $name type must be > 0 (is $size).")
        return FixedLongArraySerializer(size) as Serializer<Value<LongArray>>
    }
}

@Suppress("UNCHECKED_CAST")
object FloatArrayType : Type<FloatArray>() {
    override val name = "FLOAT_VEC"
    override val numeric = false
    override val kotlinType: KClass<FloatArrayValue> = FloatArrayValue::class
    override fun serializer(size: Int): Serializer<Value<FloatArray>> {
        if (size <= 0) throw IllegalArgumentException("Size attribute for a $name type must be > 0 (is $size).")
        return FixedFloatVectorSerializer(size) as Serializer<Value<FloatArray>>
    }
}

@Suppress("UNCHECKED_CAST")
object DoubleArrayType : Type<DoubleArray>() {
    override val name = "DOUBLE_VEC"
    override val numeric = false
    override val kotlinType: KClass<DoubleArrayValue> = DoubleArrayValue::class
    override fun serializer(size: Int): Serializer<Value<DoubleArray>> {
        if (size <= 0) throw IllegalArgumentException("Size attribute for a $name type must be > 0 (is $size).")
        return FixedDoubleVectorSerializer(size) as Serializer<Value<DoubleArray>>
    }
}

@Suppress("UNCHECKED_CAST")
object BooleanArrayType : Type<BooleanArray>() {
    override val name = "BOOLEAN_VEC"
    override val numeric = false
    override val kotlinType: KClass<BooleanArrayValue> = BooleanArrayValue::class
    override fun serializer(size: Int): Serializer<Value<BooleanArray>> {
        if (size <= 0) throw IllegalArgumentException("Size attribute for a $name type must be > 0 (is $size).")
        return FixedBooleanVectorSerializer(size) as Serializer<Value<BooleanArray>>
    }
}

