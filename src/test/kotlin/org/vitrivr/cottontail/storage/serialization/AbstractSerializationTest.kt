package org.vitrivr.cottontail.storage.serialization

import org.junit.jupiter.params.provider.Arguments
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.config.ExecutionConfig
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.catalogue.CatalogueTest
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.model.basics.Name
import java.nio.file.Files
import java.util.*
import java.util.stream.Collectors
import java.util.stream.Stream

/**
 * An abstract class for test cases that test for correctness of [Value] serialization
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class AbstractSerializationTest {
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

    /** The [Catalogue] instance used for the [AbstractSerializationTest]. */
    protected val catalogue: Catalogue = Catalogue(TestConstants.config)

    /** The [TransactionManager] used for this [CatalogueTest] instance. */
    protected val manager = TransactionManager(ExecutionConfig())

    /** The [Schema] instance used for the [AbstractSerializationTest]. */
    protected val schema = this.catalogue.let { cat ->
        val transaction = manager.Transaction(TransactionType.USER)
        cat.Tx(transaction).use { txn ->
            val name = Name.SchemaName("schema-test")
            txn.createSchema(name)
            txn.commit()
            txn.schemaForName(name)
        }
    }

    /**
     * Closes the [Catalogue] and deletes all the files.
     */
    protected fun cleanup() {
        this.catalogue.close()
        val pathsToDelete = Files.walk(TestConstants.config.root).sorted(Comparator.reverseOrder())
            .collect(Collectors.toList())
        pathsToDelete.forEach { Files.delete(it) }
    }
}