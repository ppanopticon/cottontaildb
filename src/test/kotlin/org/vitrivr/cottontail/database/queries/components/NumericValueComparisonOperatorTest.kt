package org.vitrivr.cottontail.database.queries.components

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value

/**
 * Test case that tests for correctness of [ComparisonOperator]s.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class NumericValueComparisonOperatorTest {
    /**
     * Checks correctness of the [ComparisonOperator.ISNULL] operator.
     */
    @RepeatedTest(100)
    fun checkIsNull() {
        val referenceBoolean = BooleanValue.random()
        val referenceByte = ByteValue.random()
        val referenceShort = ShortValue.random()
        val referenceInt = IntValue.random()
        val referenceLong = LongValue.random()
        val referenceFloat = FloatValue.random()
        val referenceDouble = DoubleValue.random()
        val referenceNull = null

        /** Assert ISNULL. */
        Assertions.assertFalse(ComparisonOperator.IsNull().match(referenceBoolean))
        Assertions.assertFalse(ComparisonOperator.IsNull().match(referenceByte))
        Assertions.assertFalse(ComparisonOperator.IsNull().match(referenceShort))
        Assertions.assertFalse(ComparisonOperator.IsNull().match(referenceInt))
        Assertions.assertFalse(ComparisonOperator.IsNull().match(referenceLong))
        Assertions.assertFalse(ComparisonOperator.IsNull().match(referenceFloat))
        Assertions.assertFalse(ComparisonOperator.IsNull().match(referenceDouble))
        Assertions.assertTrue(ComparisonOperator.IsNull().match(referenceNull))
    }

    /**
     * Checks correctness of the [ComparisonOperator.EQUAL] operator.
     */
    @RepeatedTest(100)
    fun checkEqual() {
        val referenceBoolean = BooleanValue.random()
        val referenceByte = ByteValue.random()
        val referenceShort = ShortValue.random()
        val referenceInt = IntValue.random()
        val referenceLong = LongValue.random()
        val referenceFloat = FloatValue.random()
        val referenceDouble = DoubleValue.random()

        val positiveComparisonBoolean = BooleanValue(referenceBoolean.value)
        val positiveComparisonByte = ByteValue(referenceByte.value)
        val positiveComparisonShort = ShortValue(referenceShort.value)
        val positiveComparisonInt = IntValue(referenceInt.value)
        val positiveComparisonLong = LongValue(referenceLong.value)
        val positiveComparisonFloat = FloatValue(referenceFloat.value)
        val positiveComparisonDouble = DoubleValue(referenceDouble.value)

        val negativeComparisonBoolean = BooleanValue.random()
        val negativeComparisonByte = ByteValue.random()
        val negativeComparisonShort = ShortValue.random()
        val negativeComparisonInt = IntValue.random()
        val negativeComparisonLong = LongValue.random()
        val negativeComparisonFloat = FloatValue.random()
        val negativeComparisonDouble = DoubleValue.random()

        val context = BindingContext<Value>()

        /** Assert equality .*/
        Assertions.assertEquals(referenceBoolean, positiveComparisonBoolean)
        Assertions.assertTrue(ComparisonOperator.Binary.Equal(context.bind(positiveComparisonBoolean)).match(referenceBoolean))

        Assertions.assertEquals(referenceByte, positiveComparisonByte)
        Assertions.assertTrue(ComparisonOperator.Binary.Equal(context.bind(positiveComparisonByte)).match(referenceByte))

        Assertions.assertEquals(referenceShort, positiveComparisonShort)
        Assertions.assertTrue(ComparisonOperator.Binary.Equal(context.bind(positiveComparisonShort)).match(referenceShort))

        Assertions.assertEquals(referenceInt, positiveComparisonInt)
        Assertions.assertTrue(ComparisonOperator.Binary.Equal(context.bind(positiveComparisonInt)).match(referenceInt))

        Assertions.assertEquals(referenceLong, positiveComparisonLong)
        Assertions.assertTrue(ComparisonOperator.Binary.Equal(context.bind(positiveComparisonLong)).match(referenceLong))

        Assertions.assertEquals(referenceFloat, positiveComparisonFloat)
        Assertions.assertTrue(ComparisonOperator.Binary.Equal(context.bind(positiveComparisonFloat)).match(referenceFloat))

        Assertions.assertEquals(referenceDouble, positiveComparisonDouble)
        Assertions.assertTrue(ComparisonOperator.Binary.Equal(context.bind(positiveComparisonDouble)).match(referenceDouble))

        /** Assert inequality .*/
        if (referenceBoolean.value != negativeComparisonBoolean.value) {
            Assertions.assertNotEquals(referenceBoolean, negativeComparisonBoolean)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(context.bind(negativeComparisonBoolean)).match(referenceBoolean))
        }
        if (referenceByte.value != negativeComparisonByte.value) {
            Assertions.assertNotEquals(referenceByte, negativeComparisonByte)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(context.bind(negativeComparisonByte)).match(referenceByte))
        }
        if (referenceShort.value != negativeComparisonShort.value) {
            Assertions.assertNotEquals(referenceShort, negativeComparisonShort)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(context.bind(negativeComparisonShort)).match(referenceShort))
        }
        if (referenceInt.value != negativeComparisonInt.value) {
            Assertions.assertNotEquals(referenceInt, negativeComparisonInt)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(context.bind(negativeComparisonInt)).match(referenceInt))
        }
        if (referenceLong.value != negativeComparisonLong.value) {
            Assertions.assertNotEquals(referenceLong, negativeComparisonLong)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(context.bind(negativeComparisonLong)).match(referenceLong))
        }
        if (referenceFloat.value != negativeComparisonFloat.value) {
            Assertions.assertNotEquals(referenceFloat, negativeComparisonFloat)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(context.bind(negativeComparisonFloat)).match(referenceFloat))
        }
        if (referenceDouble.value != negativeComparisonDouble.value) {
            Assertions.assertNotEquals(referenceDouble, negativeComparisonDouble)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(context.bind(negativeComparisonDouble)).match(referenceDouble))
        }
    }

    /**
     * Checks correctness of the [ComparisonOperator.IN] operator.
     */
    @RepeatedTest(100)
    fun checkIn() {
        val referenceShort = ShortValue.random()
        val referenceInt = IntValue.random()
        val referenceLong = LongValue.random()
        val referenceFloat = FloatValue.random()
        val referenceDouble = DoubleValue.random()

        val negativeComparisonShort = ShortValue.random()
        val negativeComparisonInt = IntValue.random()
        val negativeComparisonLong = LongValue.random()
        val negativeComparisonFloat = FloatValue.random()
        val negativeComparisonDouble = DoubleValue.random()

        val context = BindingContext<Value>()
        val positiveReference = mutableListOf(
            context.bind(referenceShort),
            context.bind(referenceInt),
            context.bind(referenceLong),
            context.bind(referenceFloat),
            context.bind(referenceDouble),
            context.bind(negativeComparisonShort),
            context.bind(negativeComparisonInt),
            context.bind(negativeComparisonLong),
            context.bind(negativeComparisonFloat),
            context.bind(negativeComparisonDouble)
        )
        val negativeReference = mutableListOf(
            context.bind(negativeComparisonShort),
            context.bind(negativeComparisonInt),
            context.bind(negativeComparisonLong),
            context.bind(negativeComparisonFloat),
            context.bind(negativeComparisonDouble)
        )

        /** Assert positive IN .*/
        Assertions.assertTrue(ComparisonOperator.In(positiveReference).match(referenceShort))
        Assertions.assertTrue(ComparisonOperator.In(positiveReference).match(referenceInt))
        Assertions.assertTrue(ComparisonOperator.In(positiveReference).match(referenceLong))
        Assertions.assertTrue(ComparisonOperator.In(positiveReference).match(referenceFloat))
        Assertions.assertTrue(ComparisonOperator.In(positiveReference).match(referenceDouble))

        /** Assert negative IN .*/
        Assertions.assertFalse(ComparisonOperator.In(negativeReference).match(referenceShort))
        Assertions.assertFalse(ComparisonOperator.In(negativeReference).match(referenceInt))
        Assertions.assertFalse(ComparisonOperator.In(negativeReference).match(referenceLong))
        Assertions.assertFalse(ComparisonOperator.In(negativeReference).match(referenceFloat))
        Assertions.assertFalse(ComparisonOperator.In(negativeReference).match(referenceDouble))
    }


    /**
     * Checks correctness of the [ComparisonOperator.GREATER] and [ComparisonOperator.LESS] operator.
     */
    @RepeatedTest(100)
    fun checkGreaterOrLess() {
        val referenceByte = ByteValue.random()
        val referenceShort = ShortValue.random()
        val referenceInt = IntValue.random()
        val referenceLong = LongValue.random()
        val referenceFloat = FloatValue.random()
        val referenceDouble = DoubleValue.random()

        val comparisonByte = ByteValue.random()
        val comparisonShort = ShortValue.random()
        val comparisonInt = IntValue.random()
        val comparisonLong = LongValue.random()
        val comparisonFloat = FloatValue.random()
        val comparisonDouble = DoubleValue.random()

        val context = BindingContext<Value>()
        val comparisonByteBinding = context.bind(comparisonByte)
        val comparisonShortBinding = context.bind(comparisonShort)
        val comparisonIntBinding = context.bind(comparisonInt)
        val comparisonLongBinding = context.bind(comparisonLong)
        val comparisonFloatBinding = context.bind(comparisonFloat)
        val comparisonDoubleBinding = context.bind(comparisonDouble)

        /** Assert inequality (Byte).*/
        if (referenceByte.value > comparisonByte.value) {
            Assertions.assertNotEquals(referenceByte, comparisonByte)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(comparisonByteBinding).match(referenceByte))
            Assertions.assertTrue(ComparisonOperator.Binary.Greater(comparisonByteBinding).match(referenceByte))
            Assertions.assertTrue(ComparisonOperator.Binary.GreaterEqual(comparisonByteBinding).match(referenceByte))
            Assertions.assertFalse(ComparisonOperator.Binary.Less(comparisonByteBinding).match(referenceByte))
            Assertions.assertFalse(ComparisonOperator.Binary.LessEqual(comparisonByteBinding).match(referenceByte))
        } else if (referenceByte.value < comparisonByte.value) {
            Assertions.assertNotEquals(referenceByte, comparisonByte)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(comparisonByteBinding).match(referenceByte))
            Assertions.assertFalse(ComparisonOperator.Binary.Greater(comparisonByteBinding).match(referenceByte))
            Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(comparisonByteBinding).match(referenceByte))
            Assertions.assertTrue(ComparisonOperator.Binary.Less(comparisonByteBinding).match(referenceByte))
            Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(comparisonByteBinding).match(referenceByte))
        } else {
            Assertions.assertEquals(referenceByte, comparisonByte)
            Assertions.assertTrue(ComparisonOperator.Binary.Equal(comparisonByteBinding).match(referenceByte))
            Assertions.assertFalse(ComparisonOperator.Binary.Greater(comparisonByteBinding).match(referenceByte))
            Assertions.assertTrue(ComparisonOperator.Binary.GreaterEqual(comparisonByteBinding).match(referenceByte))
            Assertions.assertFalse(ComparisonOperator.Binary.Less(comparisonByteBinding).match(referenceByte))
            Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(comparisonByteBinding).match(referenceByte))
        }

        /** Assert inequality (Short) .*/
        if (referenceShort.value > comparisonShort.value) {
            Assertions.assertNotEquals(referenceShort, comparisonShort)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(comparisonShortBinding).match(referenceShort))
            Assertions.assertTrue(ComparisonOperator.Binary.Greater(comparisonShortBinding).match(referenceShort))
            Assertions.assertTrue(ComparisonOperator.Binary.GreaterEqual(comparisonShortBinding).match(referenceShort))
            Assertions.assertFalse(ComparisonOperator.Binary.Less(comparisonShortBinding).match(referenceShort))
            Assertions.assertFalse(ComparisonOperator.Binary.LessEqual(comparisonShortBinding).match(referenceShort))
        } else if (referenceShort.value < comparisonShort.value) {
            Assertions.assertNotEquals(referenceShort, comparisonShort)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(comparisonShortBinding).match(referenceShort))
            Assertions.assertFalse(ComparisonOperator.Binary.Greater(comparisonShortBinding).match(referenceShort))
            Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(comparisonShortBinding).match(referenceShort))
            Assertions.assertTrue(ComparisonOperator.Binary.Less(comparisonShortBinding).match(referenceShort))
            Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(comparisonShortBinding).match(referenceShort))
        } else {
            Assertions.assertEquals(referenceShort, comparisonShort)
            Assertions.assertTrue(ComparisonOperator.Binary.Equal(comparisonShortBinding).match(referenceShort))
            Assertions.assertFalse(ComparisonOperator.Binary.Greater(comparisonShortBinding).match(referenceShort))
            Assertions.assertTrue(ComparisonOperator.Binary.GreaterEqual(comparisonShortBinding).match(referenceShort))
            Assertions.assertFalse(ComparisonOperator.Binary.Less(comparisonShortBinding).match(referenceShort))
            Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(comparisonShortBinding).match(referenceShort))
        }

        /** Assert inequality (Int) .*/
        if (referenceInt.value > comparisonInt.value) {
            Assertions.assertNotEquals(referenceInt, comparisonInt)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(comparisonIntBinding).match(referenceInt))
            Assertions.assertTrue(ComparisonOperator.Binary.Greater(comparisonIntBinding).match(referenceInt))
            Assertions.assertTrue(ComparisonOperator.Binary.GreaterEqual(comparisonIntBinding).match(referenceInt))
            Assertions.assertFalse(ComparisonOperator.Binary.Less(comparisonIntBinding).match(referenceInt))
            Assertions.assertFalse(ComparisonOperator.Binary.LessEqual(comparisonIntBinding).match(referenceInt))
        } else if (referenceInt.value < comparisonInt.value) {
            Assertions.assertNotEquals(referenceInt, comparisonInt)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(comparisonIntBinding).match(referenceInt))
            Assertions.assertFalse(ComparisonOperator.Binary.Greater(comparisonIntBinding).match(referenceInt))
            Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(comparisonIntBinding).match(referenceInt))
            Assertions.assertTrue(ComparisonOperator.Binary.Less(comparisonIntBinding).match(referenceInt))
            Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(comparisonIntBinding).match(referenceInt))
        } else {
            Assertions.assertNotEquals(referenceInt, comparisonInt)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(comparisonIntBinding).match(referenceInt))
            Assertions.assertFalse(ComparisonOperator.Binary.Greater(comparisonIntBinding).match(referenceInt))
            Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(comparisonIntBinding).match(referenceInt))
            Assertions.assertTrue(ComparisonOperator.Binary.Less(comparisonIntBinding).match(referenceInt))
            Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(comparisonIntBinding).match(referenceInt))
        }

        /** Assert inequality (Long) .*/
        if (referenceLong.value > comparisonLong.value) {
            Assertions.assertNotEquals(referenceLong, comparisonLong)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(comparisonLongBinding).match(referenceLong))
            Assertions.assertTrue(ComparisonOperator.Binary.Greater(comparisonLongBinding).match(referenceLong))
            Assertions.assertTrue(ComparisonOperator.Binary.GreaterEqual(comparisonLongBinding).match(referenceLong))
            Assertions.assertFalse(ComparisonOperator.Binary.Less(comparisonLongBinding).match(referenceLong))
            Assertions.assertFalse(ComparisonOperator.Binary.LessEqual(comparisonLongBinding).match(referenceLong))
        } else if (referenceLong.value < comparisonLong.value) {
            Assertions.assertNotEquals(referenceLong, comparisonLong)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(comparisonLongBinding).match(referenceLong))
            Assertions.assertFalse(ComparisonOperator.Binary.Greater(comparisonLongBinding).match(referenceLong))
            Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(comparisonLongBinding).match(referenceLong))
            Assertions.assertTrue(ComparisonOperator.Binary.Less(comparisonLongBinding).match(referenceLong))
            Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(comparisonLongBinding).match(referenceLong))
        } else {
            Assertions.assertNotEquals(referenceLong, comparisonLong)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(comparisonLongBinding).match(referenceLong))
            Assertions.assertFalse(ComparisonOperator.Binary.Greater(comparisonLongBinding).match(referenceLong))
            Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(comparisonLongBinding).match(referenceLong))
            Assertions.assertTrue(ComparisonOperator.Binary.Less(comparisonLongBinding).match(referenceLong))
            Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(comparisonLongBinding).match(referenceLong))
        }

        /** Assert inequality (Float) .*/
        if (referenceFloat.value > comparisonFloat.value) {
            Assertions.assertNotEquals(referenceFloat, comparisonFloat)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(comparisonFloatBinding).match(referenceFloat))
            Assertions.assertTrue(ComparisonOperator.Binary.Greater(comparisonFloatBinding).match(referenceFloat))
            Assertions.assertTrue(ComparisonOperator.Binary.GreaterEqual(comparisonFloatBinding).match(referenceFloat))
            Assertions.assertFalse(ComparisonOperator.Binary.Less(comparisonFloatBinding).match(referenceFloat))
            Assertions.assertFalse(ComparisonOperator.Binary.LessEqual(comparisonFloatBinding).match(referenceFloat))
        } else if (referenceFloat.value < comparisonFloat.value) {
            Assertions.assertNotEquals(referenceFloat, comparisonFloat)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(comparisonFloatBinding).match(referenceFloat))
            Assertions.assertFalse(ComparisonOperator.Binary.Greater(comparisonFloatBinding).match(referenceFloat))
            Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(comparisonFloatBinding).match(referenceFloat))
            Assertions.assertTrue(ComparisonOperator.Binary.Less(comparisonFloatBinding).match(referenceFloat))
            Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(comparisonFloatBinding).match(referenceFloat))
        } else {
            Assertions.assertNotEquals(referenceFloat, comparisonFloat)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(comparisonFloatBinding).match(referenceFloat))
            Assertions.assertFalse(ComparisonOperator.Binary.Greater(comparisonFloatBinding).match(referenceFloat))
            Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(comparisonFloatBinding).match(referenceFloat))
            Assertions.assertTrue(ComparisonOperator.Binary.Less(comparisonFloatBinding).match(referenceFloat))
            Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(comparisonFloatBinding).match(referenceFloat))
        }

        /** Assert inequality (Double) .*/
        if (referenceDouble.value > comparisonDouble.value) {
            Assertions.assertNotEquals(referenceDouble, comparisonDouble)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(comparisonDoubleBinding).match(referenceDouble))
            Assertions.assertTrue(ComparisonOperator.Binary.Greater(comparisonDoubleBinding).match(referenceDouble))
            Assertions.assertTrue(ComparisonOperator.Binary.GreaterEqual(comparisonDoubleBinding).match(referenceDouble))
            Assertions.assertFalse(ComparisonOperator.Binary.Less(comparisonDoubleBinding).match(referenceDouble))
            Assertions.assertFalse(ComparisonOperator.Binary.LessEqual(comparisonDoubleBinding).match(referenceDouble))
        } else if (referenceDouble.value < comparisonDouble.value) {
            Assertions.assertNotEquals(referenceDouble, comparisonDouble)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(comparisonDoubleBinding).match(referenceDouble))
            Assertions.assertFalse(ComparisonOperator.Binary.Greater(comparisonDoubleBinding).match(referenceDouble))
            Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(comparisonDoubleBinding).match(referenceDouble))
            Assertions.assertTrue(ComparisonOperator.Binary.Less(comparisonDoubleBinding).match(referenceDouble))
            Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(comparisonDoubleBinding).match(referenceDouble))
        } else {
            Assertions.assertNotEquals(referenceDouble, comparisonDouble)
            Assertions.assertFalse(ComparisonOperator.Binary.Equal(comparisonDoubleBinding).match(referenceDouble))
            Assertions.assertFalse(ComparisonOperator.Binary.Greater(comparisonDoubleBinding).match(referenceDouble))
            Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(comparisonDoubleBinding).match(referenceDouble))
            Assertions.assertTrue(ComparisonOperator.Binary.Less(comparisonDoubleBinding).match(referenceDouble))
            Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(comparisonDoubleBinding).match(referenceDouble))
        }
    }

    /**
     * Checks correctness of the [ComparisonOperator.BETWEEN] operator.
     */
    @RepeatedTest(100)
    fun checkBetween() {
        val referenceInt = IntValue.random()
        val referenceLong = LongValue.random()
        val referenceFloat = FloatValue.random()
        val referenceDouble = DoubleValue.random()

        /* Bind values. */
        val context = BindingContext<Value>()
        val comparisonLowerInt = context.bind(IntValue(Value.RANDOM.nextInt(Int.MIN_VALUE, referenceInt.value)))
        val comparisonLowerLong = context.bind(LongValue(Value.RANDOM.nextLong(Long.MIN_VALUE, referenceLong.value)))
        val comparisonLowerFloat = context.bind(FloatValue(Value.RANDOM.nextDouble(Double.MIN_VALUE, referenceFloat.value.toDouble())))
        val comparisonLowerDouble = context.bind(DoubleValue(Value.RANDOM.nextDouble(Double.MIN_VALUE, referenceDouble.value)))

        val comparisonUpperInt = context.bind(IntValue(Value.RANDOM.nextInt(referenceInt.value, Int.MAX_VALUE)))
        val comparisonUpperLong = context.bind(LongValue(Value.RANDOM.nextLong(referenceLong.value, Long.MAX_VALUE)))
        val comparisonUpperFloat = context.bind(FloatValue(Value.RANDOM.nextDouble(referenceFloat.value.toDouble(), Double.MAX_VALUE)))
        val comparisonUpperDouble = context.bind(DoubleValue(Value.RANDOM.nextDouble(referenceDouble.value, Double.MAX_VALUE)))


        /** Assert BETWEEN .*/
        Assertions.assertTrue(ComparisonOperator.Between(comparisonLowerInt, comparisonUpperInt).match(referenceInt))
        Assertions.assertFalse(ComparisonOperator.Between(comparisonUpperInt, comparisonLowerInt).match(referenceInt))

        Assertions.assertTrue(ComparisonOperator.Between(comparisonLowerLong, comparisonUpperLong).match(referenceLong))
        Assertions.assertFalse(ComparisonOperator.Between(comparisonUpperLong, comparisonLowerLong).match(referenceLong))

        Assertions.assertTrue(ComparisonOperator.Between(comparisonLowerFloat, comparisonUpperFloat).match(referenceFloat))
        Assertions.assertFalse(ComparisonOperator.Between(comparisonUpperFloat, comparisonLowerFloat).match(referenceFloat))

        Assertions.assertTrue(ComparisonOperator.Between(comparisonLowerDouble, comparisonUpperDouble).match(referenceDouble))
        Assertions.assertFalse(ComparisonOperator.Between(comparisonUpperDouble, comparisonLowerDouble).match(referenceDouble))
    }
}