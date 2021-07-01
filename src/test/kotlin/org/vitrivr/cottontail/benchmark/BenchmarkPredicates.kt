package org.vitrivr.cottontail.benchmark

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.AbstractIndexTest
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.embedded
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.utilities.io.TxFileUtilities
import kotlin.time.ExperimentalTime

/**
 * Benchmarks simple WHERE clauses.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@ExperimentalTime
class BenchmarkPredicates {

    /* Test config. */
    private val config = TestConstants.testConfig()

    @ExperimentalTime
            /** Embedded Cottontail DB instance used for testing. */
    val embedded = embedded(config)

    /** [Name.SchemaName] of the test schema. */
    protected val schemaName = Name.SchemaName("test")

    /** [Name.EntityName] of the test schema. */
    protected val entityName = schemaName.entity("predicates")


    /**
     * Initializes this [AbstractIndexTest] and prepares required [Entity] and [Index].
     */
    @BeforeAll
    protected fun initialize() {
        /* Prepare data structures. */
        prepareSchema()
        prepareEntity()

        /* Populate database with data. */
        populateDatabase()
    }

    @AfterAll
    fun teardown() {
        this.embedded.stop()
        TxFileUtilities.delete(this.config.root)
    }

    /**
     * Prepares and returns an empty test [Schema].
     */
    protected fun prepareSchema() {

    }

    /**
     * Prepares and returns an empty test [Entity].
     */
    protected fun prepareEntity() {

    }

    /**
     * Prepares and returns an empty test [Entity].
     */
    protected fun populateDatabase() {

    }


}

