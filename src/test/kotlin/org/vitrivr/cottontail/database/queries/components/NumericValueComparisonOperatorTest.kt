package org.vitrivr.cottontail.database.queries.components

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
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
        Assertions.assertFalse(ComparisonOperator.ISNULL.match(referenceBoolean, emptyList()))
        Assertions.assertFalse(ComparisonOperator.ISNULL.match(referenceByte, emptyList()))
        Assertions.assertFalse(ComparisonOperator.ISNULL.match(referenceShort, emptyList()))
        Assertions.assertFalse(ComparisonOperator.ISNULL.match(referenceInt, emptyList()))
        Assertions.assertFalse(ComparisonOperator.ISNULL.match(referenceLong, emptyList()))
        Assertions.assertFalse(ComparisonOperator.ISNULL.match(referenceFloat, emptyList()))
        Assertions.assertFalse(ComparisonOperator.ISNULL.match(referenceDouble, emptyList()))
        Assertions.assertTrue(ComparisonOperator.ISNULL.match(referenceNull, emptyList()))
    }

    /**
     * Checks correctness of the [ComparisonOperator.ISNOTNULL] operator.
     */
    @RepeatedTest(100)
    fun checkIsNotNull() {
        val referenceBoolean = BooleanValue.random()
        val referenceByte = ByteValue.random()
        val referenceShort = ShortValue.random()
        val referenceInt = IntValue.random()
        val referenceLong = LongValue.random()
        val referenceFloat = FloatValue.random()
        val referenceDouble = DoubleValue.random()
        val referenceNull = null

        /** Assert ISNULL. */
        Assertions.assertTrue(ComparisonOperator.ISNOTNULL.match(referenceBoolean, emptyList()))
        Assertions.assertTrue(ComparisonOperator.ISNOTNULL.match(referenceByte, emptyList()))
        Assertions.assertTrue(ComparisonOperator.ISNOTNULL.match(referenceShort, emptyList()))
        Assertions.assertTrue(ComparisonOperator.ISNOTNULL.match(referenceInt, emptyList()))
        Assertions.assertTrue(ComparisonOperator.ISNOTNULL.match(referenceLong, emptyList()))
        Assertions.assertTrue(ComparisonOperator.ISNOTNULL.match(referenceFloat, emptyList()))
        Assertions.assertTrue(ComparisonOperator.ISNOTNULL.match(referenceDouble, emptyList()))
        Assertions.assertFalse(ComparisonOperator.ISNOTNULL.match(referenceNull, emptyList()))
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

        /** Assert equality .*/
        Assertions.assertEquals(referenceBoolean, positiveComparisonBoolean)
        Assertions.assertTrue(ComparisonOperator.EQUAL.match(referenceBoolean, listOf(positiveComparisonBoolean)))

        Assertions.assertEquals(referenceByte, positiveComparisonByte)
        Assertions.assertTrue(ComparisonOperator.EQUAL.match(referenceByte, listOf(positiveComparisonByte)))

        Assertions.assertEquals(referenceShort, positiveComparisonShort)
        Assertions.assertTrue(ComparisonOperator.EQUAL.match(referenceShort, listOf(positiveComparisonShort)))

        Assertions.assertEquals(referenceInt, positiveComparisonInt)
        Assertions.assertTrue(ComparisonOperator.EQUAL.match(referenceInt, listOf(positiveComparisonInt)))

        Assertions.assertEquals(referenceLong, positiveComparisonLong)
        Assertions.assertTrue(ComparisonOperator.EQUAL.match(referenceLong, listOf(positiveComparisonLong)))

        Assertions.assertEquals(referenceFloat, positiveComparisonFloat)
        Assertions.assertTrue(ComparisonOperator.EQUAL.match(referenceFloat, listOf(positiveComparisonFloat)))

        Assertions.assertEquals(referenceDouble, positiveComparisonDouble)
        Assertions.assertTrue(ComparisonOperator.EQUAL.match(referenceDouble, listOf(positiveComparisonDouble)))

        /** Assert inequality .*/
        if (referenceBoolean.value != negativeComparisonBoolean.value) {
            Assertions.assertNotEquals(referenceBoolean, negativeComparisonBoolean)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceBoolean, listOf(negativeComparisonBoolean)))
        }
        if (referenceByte.value != negativeComparisonByte.value) {
            Assertions.assertNotEquals(referenceByte, negativeComparisonByte)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceByte, listOf(negativeComparisonByte)))
        }
        if (referenceShort.value != negativeComparisonShort.value) {
            Assertions.assertNotEquals(referenceShort, negativeComparisonShort)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceShort, listOf(negativeComparisonShort)))
        }
        if (referenceInt.value != negativeComparisonInt.value) {
            Assertions.assertNotEquals(referenceInt, negativeComparisonInt)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceInt, listOf(negativeComparisonInt)))
        }
        if (referenceLong.value != negativeComparisonLong.value) {
            Assertions.assertNotEquals(referenceLong, negativeComparisonLong)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceLong, listOf(negativeComparisonLong)))
        }
        if (referenceFloat.value != negativeComparisonFloat.value) {
            Assertions.assertNotEquals(referenceFloat, negativeComparisonFloat)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceFloat, listOf(negativeComparisonFloat)))
        }
        if (referenceDouble.value != negativeComparisonDouble.value) {
            Assertions.assertNotEquals(referenceDouble, negativeComparisonDouble)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceDouble, listOf(negativeComparisonDouble)))
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

        val positiveReference = setOf<Value>(referenceShort, referenceInt, referenceLong, referenceFloat, referenceDouble, negativeComparisonShort, negativeComparisonInt, negativeComparisonLong, negativeComparisonFloat, negativeComparisonDouble)
        val negativeReference = setOf<Value>(negativeComparisonShort, negativeComparisonInt, negativeComparisonLong, negativeComparisonFloat, negativeComparisonDouble)

        /** Assert positive IN .*/
        Assertions.assertTrue(ComparisonOperator.IN.match(referenceShort, positiveReference))
        Assertions.assertTrue(ComparisonOperator.IN.match(referenceInt, positiveReference))
        Assertions.assertTrue(ComparisonOperator.IN.match(referenceLong, positiveReference))
        Assertions.assertTrue(ComparisonOperator.IN.match(referenceFloat, positiveReference))
        Assertions.assertTrue(ComparisonOperator.IN.match(referenceDouble, positiveReference))

        /** Assert negative IN .*/
        Assertions.assertFalse(ComparisonOperator.IN.match(referenceShort, negativeReference))
        Assertions.assertFalse(ComparisonOperator.IN.match(referenceInt, negativeReference))
        Assertions.assertFalse(ComparisonOperator.IN.match(referenceLong, negativeReference))
        Assertions.assertFalse(ComparisonOperator.IN.match(referenceFloat, negativeReference))
        Assertions.assertFalse(ComparisonOperator.IN.match(referenceDouble, negativeReference))
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

        val comparisonByte = ByteValue(referenceByte.value)
        val comparisonShort = ShortValue(referenceShort.value)
        val comparisonInt = IntValue(referenceInt.value)
        val comparisonLong = LongValue(referenceLong.value)
        val comparisonFloat = FloatValue(referenceFloat.value)
        val comparisonDouble = DoubleValue(referenceDouble.value)

        /** Assert inequality .*/
        if (referenceByte.value > comparisonByte.value) {
            Assertions.assertNotEquals(referenceByte, comparisonByte)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceByte, listOf(comparisonByte)))
            Assertions.assertTrue(ComparisonOperator.GREATER.match(referenceByte, listOf(comparisonByte)))
            Assertions.assertFalse(ComparisonOperator.LESS.match(referenceByte, listOf(comparisonByte)))
        } else if (referenceByte.value < comparisonByte.value) {
            Assertions.assertNotEquals(referenceByte, comparisonByte)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceByte, listOf(comparisonByte)))
            Assertions.assertTrue(ComparisonOperator.LESS.match(referenceByte, listOf(comparisonByte)))
            Assertions.assertFalse(ComparisonOperator.GREATER.match(referenceByte, listOf(comparisonByte)))
        }

        /** Assert inequality .*/
        if (referenceShort.value > comparisonShort.value) {
            Assertions.assertNotEquals(referenceShort, comparisonShort)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceShort, listOf(comparisonShort)))
            Assertions.assertTrue(ComparisonOperator.GREATER.match(referenceShort, listOf(comparisonShort)))
            Assertions.assertFalse(ComparisonOperator.LESS.match(referenceShort, listOf(comparisonShort)))
        } else if (referenceShort.value < comparisonShort.value) {
            Assertions.assertNotEquals(referenceShort, comparisonShort)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceShort, listOf(comparisonShort)))
            Assertions.assertTrue(ComparisonOperator.LESS.match(referenceShort, listOf(comparisonShort)))
            Assertions.assertFalse(ComparisonOperator.GREATER.match(referenceShort, listOf(comparisonShort)))
        }

        /** Assert inequality .*/
        if (referenceInt.value > comparisonInt.value) {
            Assertions.assertNotEquals(referenceInt, comparisonInt)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceInt, listOf(comparisonInt)))
            Assertions.assertTrue(ComparisonOperator.GREATER.match(referenceInt, listOf(comparisonInt)))
            Assertions.assertFalse(ComparisonOperator.LESS.match(referenceInt, listOf(comparisonInt)))
        } else if (referenceShort.value < comparisonShort.value) {
            Assertions.assertNotEquals(referenceInt, comparisonInt)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceInt, listOf(comparisonInt)))
            Assertions.assertTrue(ComparisonOperator.LESS.match(referenceInt, listOf(comparisonInt)))
            Assertions.assertFalse(ComparisonOperator.GREATER.match(referenceInt, listOf(comparisonInt)))
        }

        /** Assert inequality .*/
        if (referenceLong.value > comparisonLong.value) {
            Assertions.assertNotEquals(referenceLong, comparisonLong)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceLong, listOf(comparisonLong)))
            Assertions.assertTrue(ComparisonOperator.GREATER.match(referenceLong, listOf(comparisonLong)))
            Assertions.assertFalse(ComparisonOperator.LESS.match(referenceLong, listOf(comparisonLong)))
        } else if (referenceShort.value < comparisonShort.value) {
            Assertions.assertNotEquals(referenceLong, comparisonLong)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceLong, listOf(comparisonLong)))
            Assertions.assertTrue(ComparisonOperator.LESS.match(referenceLong, listOf(comparisonLong)))
            Assertions.assertFalse(ComparisonOperator.GREATER.match(referenceLong, listOf(comparisonLong)))
        }

        /** Assert inequality .*/
        if (referenceFloat.value > comparisonFloat.value) {
            Assertions.assertNotEquals(referenceFloat, comparisonFloat)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceFloat, listOf(comparisonFloat)))
            Assertions.assertTrue(ComparisonOperator.GREATER.match(referenceFloat, listOf(comparisonFloat)))
            Assertions.assertFalse(ComparisonOperator.LESS.match(referenceFloat, listOf(comparisonFloat)))
        } else if (referenceShort.value < comparisonShort.value) {
            Assertions.assertNotEquals(referenceFloat, comparisonFloat)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceFloat, listOf(comparisonFloat)))
            Assertions.assertTrue(ComparisonOperator.LESS.match(referenceFloat, listOf(comparisonFloat)))
            Assertions.assertFalse(ComparisonOperator.GREATER.match(referenceFloat, listOf(comparisonFloat)))
        }

        /** Assert inequality .*/
        if (referenceDouble.value > comparisonDouble.value) {
            Assertions.assertNotEquals(referenceDouble, comparisonDouble)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceDouble, listOf(comparisonDouble)))
            Assertions.assertTrue(ComparisonOperator.GREATER.match(referenceDouble, listOf(comparisonDouble)))
            Assertions.assertFalse(ComparisonOperator.LESS.match(referenceDouble, listOf(comparisonDouble)))
        } else if (referenceShort.value < comparisonShort.value) {
            Assertions.assertNotEquals(referenceDouble, comparisonDouble)
            Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceDouble, listOf(comparisonDouble)))
            Assertions.assertTrue(ComparisonOperator.LESS.match(referenceDouble, listOf(comparisonDouble)))
            Assertions.assertFalse(ComparisonOperator.GREATER.match(referenceDouble, listOf(comparisonDouble)))
        }
    }

    /**
     * Checks correctness of the [ComparisonOperator.GEQUAL] and [ComparisonOperator.LEQUAL] operator.
     */
    @RepeatedTest(100)
    fun checkGreaterEqualOrLessEqual() {
        val referenceByte = ByteValue.random()
        val referenceShort = ShortValue.random()
        val referenceInt = IntValue.random()
        val referenceLong = LongValue.random()
        val referenceFloat = FloatValue.random()
        val referenceDouble = DoubleValue.random()

        val comparisonByte = ByteValue(referenceByte.value)
        val comparisonShort = ShortValue(referenceShort.value)
        val comparisonInt = IntValue(referenceInt.value)
        val comparisonLong = LongValue(referenceLong.value)
        val comparisonFloat = FloatValue(referenceFloat.value)
        val comparisonDouble = DoubleValue(referenceDouble.value)

        /** Assert inequality .*/
        when {
            referenceByte.value > comparisonByte.value -> {
                Assertions.assertNotEquals(referenceByte, comparisonByte)
                Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceByte, listOf(comparisonByte)))
                Assertions.assertTrue(ComparisonOperator.GREATER.match(referenceByte, listOf(comparisonByte)))
                Assertions.assertTrue(ComparisonOperator.GEQUAL.match(referenceByte, listOf(comparisonByte)))
                Assertions.assertFalse(ComparisonOperator.LESS.match(referenceByte, listOf(comparisonByte)))
                Assertions.assertFalse(ComparisonOperator.LEQUAL.match(referenceByte, listOf(comparisonByte)))
            }
            referenceByte.value < comparisonByte.value -> {
                Assertions.assertNotEquals(referenceByte, comparisonByte)
                Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceByte, listOf(comparisonByte)))
                Assertions.assertTrue(ComparisonOperator.LESS.match(referenceByte, listOf(comparisonByte)))
                Assertions.assertTrue(ComparisonOperator.LEQUAL.match(referenceByte, listOf(comparisonByte)))
                Assertions.assertFalse(ComparisonOperator.GREATER.match(referenceByte, listOf(comparisonByte)))
                Assertions.assertFalse(ComparisonOperator.GEQUAL.match(referenceByte, listOf(comparisonByte)))
            }
            referenceByte.value == comparisonByte.value -> {
                Assertions.assertEquals(referenceByte, comparisonByte)
                Assertions.assertTrue(ComparisonOperator.EQUAL.match(referenceByte, listOf(comparisonByte)))
                Assertions.assertFalse(ComparisonOperator.LESS.match(referenceByte, listOf(comparisonByte)))
                Assertions.assertFalse(ComparisonOperator.GREATER.match(referenceByte, listOf(comparisonByte)))
                Assertions.assertTrue(ComparisonOperator.LEQUAL.match(referenceByte, listOf(comparisonByte)))
                Assertions.assertTrue(ComparisonOperator.GEQUAL.match(referenceByte, listOf(comparisonByte)))
            }
        }

        when {
            referenceShort.value > comparisonShort.value -> {
                Assertions.assertNotEquals(referenceShort, comparisonShort)
                Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceShort, listOf(comparisonShort)))
                Assertions.assertTrue(ComparisonOperator.GREATER.match(referenceShort, listOf(comparisonShort)))
                Assertions.assertTrue(ComparisonOperator.GEQUAL.match(referenceShort, listOf(comparisonShort)))
                Assertions.assertFalse(ComparisonOperator.LESS.match(referenceShort, listOf(comparisonShort)))
                Assertions.assertFalse(ComparisonOperator.LEQUAL.match(referenceShort, listOf(comparisonShort)))
            }
            referenceShort.value < comparisonShort.value -> {
                Assertions.assertNotEquals(referenceShort, comparisonShort)
                Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceShort, listOf(comparisonShort)))
                Assertions.assertTrue(ComparisonOperator.LESS.match(referenceShort, listOf(comparisonShort)))
                Assertions.assertTrue(ComparisonOperator.LEQUAL.match(referenceShort, listOf(comparisonShort)))
                Assertions.assertFalse(ComparisonOperator.GREATER.match(referenceShort, listOf(comparisonShort)))
                Assertions.assertFalse(ComparisonOperator.GEQUAL.match(referenceShort, listOf(comparisonShort)))
            }
            referenceShort.value == comparisonShort.value -> {
                Assertions.assertEquals(referenceShort, comparisonShort)
                Assertions.assertTrue(ComparisonOperator.EQUAL.match(referenceShort, listOf(comparisonShort)))
                Assertions.assertFalse(ComparisonOperator.LESS.match(referenceShort, listOf(comparisonShort)))
                Assertions.assertFalse(ComparisonOperator.GREATER.match(referenceShort, listOf(comparisonShort)))
                Assertions.assertTrue(ComparisonOperator.LEQUAL.match(referenceShort, listOf(comparisonShort)))
                Assertions.assertTrue(ComparisonOperator.GEQUAL.match(referenceShort, listOf(comparisonShort)))
            }
        }

        when {
            referenceInt.value > comparisonInt.value -> {
                Assertions.assertNotEquals(referenceInt, comparisonInt)
                Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceInt, listOf(comparisonInt)))
                Assertions.assertTrue(ComparisonOperator.GREATER.match(referenceInt, listOf(comparisonInt)))
                Assertions.assertTrue(ComparisonOperator.GEQUAL.match(referenceInt, listOf(comparisonInt)))
                Assertions.assertFalse(ComparisonOperator.LESS.match(referenceInt, listOf(comparisonInt)))
                Assertions.assertFalse(ComparisonOperator.LEQUAL.match(referenceInt, listOf(comparisonInt)))
            }
            referenceInt.value < comparisonInt.value -> {
                Assertions.assertNotEquals(referenceInt, comparisonInt)
                Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceInt, listOf(comparisonInt)))
                Assertions.assertTrue(ComparisonOperator.LESS.match(referenceInt, listOf(comparisonInt)))
                Assertions.assertTrue(ComparisonOperator.LEQUAL.match(referenceInt, listOf(comparisonInt)))
                Assertions.assertFalse(ComparisonOperator.GREATER.match(referenceInt, listOf(comparisonInt)))
                Assertions.assertFalse(ComparisonOperator.GEQUAL.match(referenceInt, listOf(comparisonInt)))
            }
            referenceInt.value == comparisonInt.value -> {
                Assertions.assertEquals(referenceInt, comparisonInt)
                Assertions.assertTrue(ComparisonOperator.EQUAL.match(referenceInt, listOf(comparisonInt)))
                Assertions.assertFalse(ComparisonOperator.LESS.match(referenceInt, listOf(comparisonInt)))
                Assertions.assertFalse(ComparisonOperator.GREATER.match(referenceInt, listOf(comparisonInt)))
                Assertions.assertTrue(ComparisonOperator.LEQUAL.match(referenceInt, listOf(comparisonInt)))
                Assertions.assertTrue(ComparisonOperator.GEQUAL.match(referenceInt, listOf(comparisonInt)))
            }
        }

        when {
            referenceLong.value > comparisonLong.value -> {
                Assertions.assertNotEquals(referenceLong, comparisonLong)
                Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceLong, listOf(comparisonLong)))
                Assertions.assertTrue(ComparisonOperator.GREATER.match(referenceLong, listOf(comparisonLong)))
                Assertions.assertTrue(ComparisonOperator.GEQUAL.match(referenceLong, listOf(comparisonLong)))
                Assertions.assertFalse(ComparisonOperator.LESS.match(referenceLong, listOf(comparisonLong)))
                Assertions.assertFalse(ComparisonOperator.LEQUAL.match(referenceLong, listOf(comparisonLong)))
            }
            referenceLong.value < comparisonLong.value -> {
                Assertions.assertNotEquals(referenceLong, comparisonLong)
                Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceLong, listOf(comparisonLong)))
                Assertions.assertTrue(ComparisonOperator.LESS.match(referenceLong, listOf(comparisonLong)))
                Assertions.assertTrue(ComparisonOperator.LEQUAL.match(referenceLong, listOf(comparisonLong)))
                Assertions.assertFalse(ComparisonOperator.GREATER.match(referenceLong, listOf(comparisonLong)))
                Assertions.assertFalse(ComparisonOperator.GEQUAL.match(referenceLong, listOf(comparisonLong)))
            }
            referenceLong.value == comparisonLong.value -> {
                Assertions.assertEquals(referenceLong, comparisonLong)
                Assertions.assertTrue(ComparisonOperator.EQUAL.match(referenceLong, listOf(comparisonLong)))
                Assertions.assertFalse(ComparisonOperator.LESS.match(referenceLong, listOf(comparisonLong)))
                Assertions.assertFalse(ComparisonOperator.GREATER.match(referenceLong, listOf(comparisonLong)))
                Assertions.assertTrue(ComparisonOperator.LEQUAL.match(referenceLong, listOf(comparisonLong)))
                Assertions.assertTrue(ComparisonOperator.GEQUAL.match(referenceLong, listOf(comparisonLong)))
            }
        }

        when {
            referenceFloat.value > comparisonFloat.value -> {
                Assertions.assertNotEquals(referenceFloat, comparisonFloat)
                Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceFloat, listOf(comparisonFloat)))
                Assertions.assertTrue(ComparisonOperator.GREATER.match(referenceFloat, listOf(comparisonFloat)))
                Assertions.assertTrue(ComparisonOperator.GEQUAL.match(referenceFloat, listOf(comparisonFloat)))
                Assertions.assertFalse(ComparisonOperator.LESS.match(referenceFloat, listOf(comparisonFloat)))
                Assertions.assertFalse(ComparisonOperator.LEQUAL.match(referenceFloat, listOf(comparisonFloat)))
            }
            referenceFloat.value < comparisonFloat.value -> {
                Assertions.assertNotEquals(referenceFloat, comparisonFloat)
                Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceFloat, listOf(comparisonFloat)))
                Assertions.assertTrue(ComparisonOperator.LESS.match(referenceFloat, listOf(comparisonFloat)))
                Assertions.assertTrue(ComparisonOperator.LEQUAL.match(referenceFloat, listOf(comparisonFloat)))
                Assertions.assertFalse(ComparisonOperator.GREATER.match(referenceFloat, listOf(comparisonFloat)))
                Assertions.assertFalse(ComparisonOperator.GEQUAL.match(referenceFloat, listOf(comparisonFloat)))
            }
            referenceFloat.value == comparisonFloat.value -> {
                Assertions.assertEquals(referenceFloat, comparisonFloat)
                Assertions.assertTrue(ComparisonOperator.EQUAL.match(referenceFloat, listOf(comparisonFloat)))
                Assertions.assertFalse(ComparisonOperator.LESS.match(referenceFloat, listOf(comparisonFloat)))
                Assertions.assertFalse(ComparisonOperator.GREATER.match(referenceFloat, listOf(comparisonFloat)))
                Assertions.assertTrue(ComparisonOperator.LEQUAL.match(referenceFloat, listOf(comparisonFloat)))
                Assertions.assertTrue(ComparisonOperator.GEQUAL.match(referenceFloat, listOf(comparisonFloat)))
            }
        }

        when {
            referenceDouble.value > comparisonDouble.value -> {
                Assertions.assertNotEquals(referenceDouble, comparisonDouble)
                Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceDouble, listOf(comparisonDouble)))
                Assertions.assertTrue(ComparisonOperator.GREATER.match(referenceDouble, listOf(comparisonDouble)))
                Assertions.assertTrue(ComparisonOperator.GEQUAL.match(referenceDouble, listOf(comparisonDouble)))
                Assertions.assertFalse(ComparisonOperator.LESS.match(referenceDouble, listOf(comparisonDouble)))
                Assertions.assertFalse(ComparisonOperator.LEQUAL.match(referenceDouble, listOf(comparisonDouble)))
            }
            referenceDouble.value < comparisonDouble.value -> {
                Assertions.assertNotEquals(referenceDouble, comparisonDouble)
                Assertions.assertFalse(ComparisonOperator.EQUAL.match(referenceDouble, listOf(comparisonDouble)))
                Assertions.assertTrue(ComparisonOperator.LESS.match(referenceDouble, listOf(comparisonDouble)))
                Assertions.assertTrue(ComparisonOperator.LEQUAL.match(referenceDouble, listOf(comparisonDouble)))
                Assertions.assertFalse(ComparisonOperator.GREATER.match(referenceDouble, listOf(comparisonDouble)))
                Assertions.assertFalse(ComparisonOperator.GEQUAL.match(referenceDouble, listOf(comparisonDouble)))
            }
            referenceDouble.value == comparisonDouble.value -> {
                Assertions.assertEquals(referenceDouble, comparisonDouble)
                Assertions.assertTrue(ComparisonOperator.EQUAL.match(referenceDouble, listOf(comparisonDouble)))
                Assertions.assertFalse(ComparisonOperator.LESS.match(referenceDouble, listOf(comparisonDouble)))
                Assertions.assertFalse(ComparisonOperator.GREATER.match(referenceDouble, listOf(comparisonDouble)))
                Assertions.assertTrue(ComparisonOperator.LEQUAL.match(referenceDouble, listOf(comparisonDouble)))
                Assertions.assertTrue(ComparisonOperator.GEQUAL.match(referenceDouble, listOf(comparisonDouble)))
            }
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

        val comparisonLowerInt = IntValue(Value.RANDOM.nextInt(Int.MIN_VALUE, referenceInt.value))
        val comparisonLowerLong = LongValue(Value.RANDOM.nextLong(Long.MIN_VALUE, referenceLong.value))
        val comparisonLowerFloat = FloatValue(Value.RANDOM.nextDouble(Double.MIN_VALUE, referenceFloat.value.toDouble()))
        val comparisonLowerDouble = DoubleValue(Value.RANDOM.nextDouble(Double.MIN_VALUE, referenceDouble.value))

        val comparisonUpperInt = IntValue(Value.RANDOM.nextInt(referenceInt.value, Int.MAX_VALUE))
        val comparisonUpperLong = LongValue(Value.RANDOM.nextLong(referenceLong.value, Long.MAX_VALUE))
        val comparisonUpperFloat = FloatValue(Value.RANDOM.nextDouble(referenceFloat.value.toDouble(), Double.MAX_VALUE))
        val comparisonUpperDouble = DoubleValue(Value.RANDOM.nextDouble(referenceDouble.value, Double.MAX_VALUE))

        /** Assert BETWEEN .*/
        Assertions.assertTrue(ComparisonOperator.BETWEEN.match(referenceInt, listOf(comparisonLowerInt, comparisonUpperInt)))
        Assertions.assertFalse(ComparisonOperator.BETWEEN.match(referenceInt, listOf(comparisonUpperInt, comparisonLowerInt)))

        Assertions.assertTrue(ComparisonOperator.BETWEEN.match(referenceLong, listOf(comparisonLowerLong, comparisonUpperLong)))
        Assertions.assertFalse(ComparisonOperator.BETWEEN.match(referenceLong, listOf(comparisonUpperLong, comparisonLowerLong)))

        Assertions.assertTrue(ComparisonOperator.BETWEEN.match(referenceFloat, listOf(comparisonLowerFloat, comparisonUpperFloat)))
        Assertions.assertFalse(ComparisonOperator.BETWEEN.match(referenceFloat, listOf(comparisonUpperFloat, comparisonLowerFloat)))

        Assertions.assertTrue(ComparisonOperator.BETWEEN.match(referenceDouble, listOf(comparisonLowerDouble, comparisonUpperDouble)))
        Assertions.assertFalse(ComparisonOperator.BETWEEN.match(referenceDouble, listOf(comparisonUpperDouble, comparisonLowerDouble)))
    }
}