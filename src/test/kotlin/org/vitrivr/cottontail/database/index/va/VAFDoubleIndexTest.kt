package org.vitrivr.cottontail.database.index.va

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.index.AbstractIndexTest
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.math.knn.metrics.DistanceKernel
import org.vitrivr.cottontail.math.knn.metrics.EuclidianDistance
import org.vitrivr.cottontail.math.knn.metrics.ManhattanDistance
import org.vitrivr.cottontail.math.knn.metrics.SquaredEuclidianDistance
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.utilities.math.KnnUtilities
import java.util.*
import java.util.stream.Stream
import kotlin.collections.ArrayList
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * This is a collection of test cases to test the correct behaviour of [VAFIndex] for [DoubleVectorValue]s.
 *
 * @author Ralph Gasser
 * @param 1.2.0
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VAFDoubleIndexTest : AbstractIndexTest() {

    companion object {
        @JvmStatic
        fun kernels(): Stream<DistanceKernel> =
            Stream.of(ManhattanDistance, EuclidianDistance, SquaredEuclidianDistance)
    }

    /** Random number generator. */
    private val random = SplittableRandom()

    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef.withAttributes(this.entityName.column("id"), "LONG", -1, false),
        ColumnDef.withAttributes(
            this.entityName.column("feature"),
            "DOUBLE_VEC",
            this.random.nextInt(128, 2048),
            false
        )
    )

    override val indexColumn: ColumnDef<DoubleVectorValue>
        get() = this.columns[1] as ColumnDef<DoubleVectorValue>

    override val indexName: Name.IndexName
        get() = this.entityName.index("idx_feature_vaf")

    override val indexType: IndexType
        get() = IndexType.VAF

    /** Random number generator. */
    private var counter: Long = 0L

    @BeforeAll
    override fun initialize() {
        super.initialize()
    }

    @AfterAll
    override fun teardown() {
        super.teardown()
    }

    @ParameterizedTest
    @MethodSource("kernels")
    @ExperimentalTime
    fun test(distance: DistanceKernel) {
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        val k = 100
        val query = DoubleVectorValue.random(this.indexColumn.logicalSize, this.random)
        val predicate = KnnPredicate(
            column = this.indexColumn,
            k = k,
            query = listOf(query),
            distance = distance
        )

        val indexTx = txn.getTx(this.index!!) as IndexTx
        val entityTx = txn.getTx(this.entity!!) as EntityTx

        /* Fetch results through index. */
        val indexResults = ArrayList<Record>(k)
        val indexDuration = measureTime {
            indexTx.filter(predicate).use { it.forEach { indexResults.add(it) } }
        }

        /* Fetch results through full table scan. */
        val bruteForceResults = MinHeapSelection<ComparablePair<TupleId, DoubleValue>>(k)
        val bruteForceDuration = measureTime {
            entityTx.scan(arrayOf(this.indexColumn)).use {
                it.forEach {
                    val vector = it[this.indexColumn]
                    if (vector is DoubleVectorValue) {
                        bruteForceResults.offer(
                            ComparablePair(
                                it.tupleId,
                                predicate.distance.invoke(query, vector)
                            )
                        )
                    }
                }
            }
        }

        /* Compare results. */
        for ((i, e) in indexResults.withIndex()) {
            Assertions.assertEquals(bruteForceResults[i].first, e.tupleId)
            Assertions.assertEquals(
                bruteForceResults[i].second,
                e[KnnUtilities.distanceColumnDef(this.entityName)]
            )
        }

        log("Test done for ${distance::class.java.simpleName}! VAF took $indexDuration, brute-force took $bruteForceDuration.")
    }

    override fun nextRecord(): StandaloneRecord {
        val id = LongValue(this.counter++)
        val vector = DoubleVectorValue.random(this.indexColumn.logicalSize, this.random)
        return StandaloneRecord(columns = this.columns, values = arrayOf(id, vector))
    }
}