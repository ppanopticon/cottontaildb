package org.vitrivr.cottontail.database.index.va

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.index.AbstractIndexTest
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.math.knn.basics.DistanceKernel
import org.vitrivr.cottontail.math.knn.kernels.Distances
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
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
 * @param 1.3.0
 */
class VAFDoubleIndexTest : AbstractIndexTest() {

    companion object {
        @JvmStatic
        fun kernels(): Stream<Distances> = Stream.of(Distances.L1, Distances.L2, Distances.L2SQUARED)
    }

    /** Random number generator. */
    private val random = SplittableRandom()

    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Type.Long),
        ColumnDef(
            this.entityName.column("feature"),
            Type.DoubleVector(this.random.nextInt(128, 2048))
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

    @ParameterizedTest
    @MethodSource("kernels")
    @ExperimentalTime
    fun test(distance: Distances) {
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        val k = 100
        val query = DoubleVectorValue.random(this.indexColumn.type.logicalSize, this.random)
        val kernel = distance.kernelForQuery(query) as DistanceKernel<VectorValue<*>>
        val context = BindingContext<Value>()
        val predicate = KnnPredicate(
            column = this.indexColumn,
            k = k,
            distance = distance,
            query = context.bind(query)
        )

        /* Obtain necessary transactions. */
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = txn.getTx(schema) as SchemaTx
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = txn.getTx(entity) as EntityTx
        val index = entityTx.indexForName(this.indexName)
        val indexTx = txn.getTx(index) as IndexTx

        /* Fetch results through index. */
        val indexResults = ArrayList<Record>(k)
        val indexDuration = measureTime {
            indexTx.filter(predicate).forEach { indexResults.add(it) }
        }

        /* Fetch results through full table scan. */
        val bruteForceResults = MinHeapSelection<ComparablePair<TupleId, DoubleValue>>(k)
        val bruteForceDuration = measureTime {
            entityTx.scan(arrayOf(this.indexColumn)).forEach {
                val vector = it[this.indexColumn]
                if (vector is DoubleVectorValue) {
                    bruteForceResults.offer(
                        ComparablePair(
                            it.tupleId,
                            kernel(vector)
                        )
                    )
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
        txn.commit()
    }

    override fun nextRecord(): StandaloneRecord {
        val id = LongValue(this.counter++)
        val vector = DoubleVectorValue.random(this.indexColumn.type.logicalSize, this.random)
        return StandaloneRecord(0L, columns = this.columns, values = arrayOf(id, vector))
    }
}