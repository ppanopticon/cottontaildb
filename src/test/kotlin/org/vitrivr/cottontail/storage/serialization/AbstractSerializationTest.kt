package org.vitrivr.cottontail.storage.serialization

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.params.provider.Arguments
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.catalogue.CatalogueTest
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.utilities.io.FileUtilities
import java.nio.file.Files
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.stream.Stream

/**
 * An abstract class for test cases that test for correctness of [Value] serialization
 *
 * @author Ralph Gasser
 * @version 1.1.1
 */
abstract class AbstractSerializationTest {
    companion object {
        /** A Random number generator used for the [AbstractSerializationTest]. */
        protected val random = SplittableRandom()

        /** Random set of dimensions used for generating test vectors. */
        @JvmStatic
        fun dimensions(): Stream<Arguments> = Stream.of(
            Arguments.of(TestConstants.smallVectorMaxDimension),
            Arguments.of(this.random.nextInt(2, TestConstants.smallVectorMaxDimension)),
            Arguments.of(TestConstants.mediumVectorMaxDimension),
            Arguments.of(this.random.nextInt(2, TestConstants.mediumVectorMaxDimension)),
            Arguments.of(this.random.nextInt(2, TestConstants.mediumVectorMaxDimension)),
            Arguments.of(TestConstants.largeVectorMaxDimension),
            Arguments.of(this.random.nextInt(2, TestConstants.largeVectorMaxDimension)),
            Arguments.of(this.random.nextInt(2, TestConstants.largeVectorMaxDimension)),
            Arguments.of(this.random.nextInt(2, TestConstants.largeVectorMaxDimension))
        )
    }

    init {
        /* Assure that root folder is empty! */
        if (Files.exists(TestConstants.config.root)) {
            FileUtilities.deleteRecursively(TestConstants.config.root)
        }
        Files.createDirectories(TestConstants.config.root)
    }

    /** The [DefaultCatalogue] instance used for the [AbstractSerializationTest]. */
    protected val catalogue: DefaultCatalogue = DefaultCatalogue(TestConstants.config)

    /** The [TransactionManager] used for this [CatalogueTest] instance. */
    protected val manager = TransactionManager(
        Executors.newFixedThreadPool(1) as ThreadPoolExecutor,
        TestConstants.config.execution.transactionTableSize,
        TestConstants.config.execution.transactionHistorySize
    )

    /** The [Schema] instance used for the [AbstractSerializationTest]. */
    protected val schema: Schema = this.catalogue.let { cat ->
        val transaction = manager.Transaction(TransactionType.USER)
        cat.Tx(transaction).use { txn ->
            val name = Name.SchemaName("schema-test")
            txn.createSchema(name)
            txn.commit()
            txn.schemaForName(name)
        }
    }

    /**
     * Closes the [DefaultCatalogue] and deletes all the files.
     */
    @AfterEach
    fun cleanup() {
        this.catalogue.close()
        FileUtilities.deleteRecursively(TestConstants.config.root)
    }
}