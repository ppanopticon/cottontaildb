package org.vitrivr.cottontail.storage.store.engine.hare.access

import org.junit.jupiter.params.provider.Arguments
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.storage.serialization.AbstractSerializationTest
import java.util.*
import java.util.stream.Stream

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class AbstractCursorTest {
    companion object {
        /** A Random number generator used for the [AbstractSerializationTest]. */
        protected val random = SplittableRandom()

        /** Random set of dimensions used for generating test vectors. */
        @JvmStatic
        fun dimensions(): Stream<Arguments> = Stream.of(
                Arguments.of(TestConstants.smallVectorMaxDimension),
                Arguments.of(this.random.nextInt(TestConstants.smallVectorMaxDimension)),
                Arguments.of(TestConstants.mediumVectorMaxDimension),
                Arguments.of(this.random.nextInt(TestConstants.mediumVectorMaxDimension)),
                Arguments.of(this.random.nextInt(TestConstants.mediumVectorMaxDimension)),
                Arguments.of(TestConstants.largeVectorMaxDimension),
                Arguments.of(this.random.nextInt(TestConstants.largeVectorMaxDimension)),
                Arguments.of(this.random.nextInt(TestConstants.largeVectorMaxDimension)),
                Arguments.of(this.random.nextInt(TestConstants.largeVectorMaxDimension))
        )
    }
}