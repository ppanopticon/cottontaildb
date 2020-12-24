package org.vitrivr.cottontail.math.knn

import org.junit.jupiter.params.provider.Arguments
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.TestConstants
import java.util.*
import java.util.stream.Stream

/**
 * Abstract class for test cases that test for correctness of some basic distance calculations.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class AbstractDistanceTest {
    companion object {
        /** A Random number generator used for the [AbstractDistanceTest]. */
        @JvmStatic
        protected val RANDOM = SplittableRandom()

        /** Random set of dimensions used for generating test vectors. */
        @JvmStatic
        fun dimensions(): Stream<Arguments> = Stream.of(
                Arguments.of(TestConstants.smallVectorMaxDimension),
                Arguments.of(RANDOM.nextInt(TestConstants.smallVectorMaxDimension)),
                Arguments.of(TestConstants.mediumVectorMaxDimension),
                Arguments.of(RANDOM.nextInt(TestConstants.mediumVectorMaxDimension)),
                Arguments.of(RANDOM.nextInt(TestConstants.mediumVectorMaxDimension)),
                Arguments.of(TestConstants.largeVectorMaxDimension),
                Arguments.of(RANDOM.nextInt(TestConstants.largeVectorMaxDimension)),
                Arguments.of(RANDOM.nextInt(TestConstants.largeVectorMaxDimension)),
                Arguments.of(RANDOM.nextInt(TestConstants.largeVectorMaxDimension))
        )

        /** Logger used for the tests. */
        @JvmStatic
        protected val LOGGER = LoggerFactory.getLogger(AbstractDistanceTest::class.java)
    }
}