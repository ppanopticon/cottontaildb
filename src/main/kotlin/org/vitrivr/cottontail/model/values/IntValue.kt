package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.database.column.Type
import org.vitrivr.cottontail.model.values.types.NumericValue
import org.vitrivr.cottontail.model.values.types.RealValue
import org.vitrivr.cottontail.model.values.types.Value
import java.util.*

/**
 * This is an abstraction over an [Int].
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
inline class IntValue(override val value: Int): RealValue<Int> {

    companion object {
        val ZERO = IntValue(0)
        val ONE = IntValue(1)

        /**
         * Generates a random [IntValue].
         *
         * @param rnd A [SplittableRandom] to generate the random numbers.
         * @return Random [IntValue]
         */
        fun random(rnd: SplittableRandom = Value.RANDOM) = IntValue(rnd.nextInt())
    }

    /**
     * Constructor for an arbitrary [Number].
     *
     * @param number The [Number] that should be converted to a [IntValue]
     */
    constructor(number: Number) : this(number.toInt())

    /**
     * Constructor for an arbitrary [NumericValue].
     *
     * @param number The [NumericValue] that should be converted to a [IntValue]
     */
    constructor(number: NumericValue<*>) : this(number.value.toInt())

    /** The logical size of this [IntValue]*/
    override val logicalSize: Int
        get() = 1

    /** The [Type] of this [IntValue]. */
    override val type: Type<*>
        get() = Type.Int

    override val real: RealValue<Int>
        get() = this

    override val imaginary: RealValue<Int>
        get() = ZERO

    /**
     * Compares this [IntValue] to another [Value]. Returns -1, 0 or 1 of other value is smaller,
     * equal or greater than this value. [IntValue] can only be compared to other [NumericValue]s.
     *
     * @param other Value to compare to.
     * @return -1, 0 or 1 of other value is smaller, equal or greater than this value
     */
    override fun compareTo(other: Value): Int = when (other) {
        is ByteValue -> this.value.compareTo(other.value)
        is ShortValue -> this.value.compareTo(other.value)
        is IntValue -> this.value.compareTo(other.value)
        is LongValue -> this.value.compareTo(other.value)
        is DoubleValue -> this.value.compareTo(other.value)
        is FloatValue -> this.value.compareTo(other.value)
        is Complex32Value -> this.value.compareTo(other.data[0])
        is Complex64Value -> this.value.compareTo(other.data[0])
        else -> throw IllegalArgumentException("LongValues can only be compared to other numeric values.")
    }

    /**
     * Checks for equality between this [IntValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [IntValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is IntValue) && (other.value == this.value)

    override fun asDouble(): DoubleValue = DoubleValue(this.value.toDouble())
    override fun asFloat(): FloatValue = FloatValue(this.value.toFloat())
    override fun asInt(): IntValue = this
    override fun asLong(): LongValue = LongValue(this.value.toLong())
    override fun asShort(): ShortValue = ShortValue(this.value.toShort())
    override fun asByte(): ByteValue = ByteValue(this.value.toByte())
    override fun asComplex32(): Complex32Value = Complex32Value(this.asFloat(), FloatValue(0.0f))
    override fun asComplex64(): Complex64Value = Complex64Value(this.asDouble(), DoubleValue(0.0))

    override fun unaryMinus(): IntValue = IntValue(-this.value)

    override fun plus(other: NumericValue<*>) = IntValue(this.value + other.value.toInt())
    override fun minus(other: NumericValue<*>) = IntValue(this.value - other.value.toInt())
    override fun times(other: NumericValue<*>) = IntValue(this.value * other.value.toInt())
    override fun div(other: NumericValue<*>) = IntValue(this.value / other.value.toInt())

    override fun abs() = IntValue(kotlin.math.abs(this.value))

    override fun pow(x: Int) = this.asDouble().pow(x)
    override fun pow(x: Double) = this.asDouble().pow(x)
    override fun sqrt() = this.asDouble().sqrt()
    override fun exp() = this.asDouble().exp()
    override fun ln() = this.asDouble().ln()

    override fun cos() = this.asDouble().cos()
    override fun sin() = this.asDouble().sin()
    override fun tan() = this.asDouble().tan()
    override fun atan() = this.asDouble().atan()
}
