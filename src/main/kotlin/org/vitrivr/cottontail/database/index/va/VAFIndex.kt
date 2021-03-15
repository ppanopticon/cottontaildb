package org.vitrivr.cottontail.database.index.va

import org.mapdb.Atomic
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.index.AbstractIndex
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.index.va.bounds.*
import org.vitrivr.cottontail.database.index.va.signature.Marks
import org.vitrivr.cottontail.database.index.va.signature.MarksGenerator
import org.vitrivr.cottontail.database.index.va.signature.VAFSignature
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.statistics.columns.*
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.math.knn.metrics.EuclidianDistance
import org.vitrivr.cottontail.math.knn.metrics.ManhattanDistance
import org.vitrivr.cottontail.math.knn.metrics.MinkowskiDistance
import org.vitrivr.cottontail.math.knn.metrics.SquaredEuclidianDistance
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import org.vitrivr.cottontail.math.knn.selection.MinSingleSelection
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.math.KnnUtilities
import java.lang.Math.floorDiv
import java.nio.file.Path
import java.util.*
import kotlin.collections.ArrayDeque
import kotlin.math.max
import kotlin.math.min

/**
 * An [AbstractIndex] structure for nearest neighbor search (NNS) that uses a vector approximation (VA) file ([1]).
 * Can be used for all types of [RealVectorValue]s and all [MinkowskiDistance] metrics.
 *
 * References:
 * [1] Weber, R. and Blott, S., 1997. An approximation based data structure for similarity search (No. 9141, p. 416). Technical Report 24, ESPRIT Project HERMES.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 2.1.1
 */
class VAFIndex(path: Path, parent: DefaultEntity, config: VAFIndexConfig? = null) : AbstractIndex(path, parent) {

    companion object {
        private const val VAF_INDEX_SIGNATURES_FIELD = "vaf_signatures"
        private const val VAF_INDEX_MARKS_FIELD = "vaf_marks"
        val LOGGER: Logger = LoggerFactory.getLogger(VAFIndex::class.java)
    }

    /** The [VAFIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = arrayOf(KnnUtilities.distanceColumnDef(this.parent.name))

    /** The type of [AbstractIndex]. */
    override val type = IndexType.VAF

    /** The [VAFIndexConfig] used by this [VAFIndex] instance. */
    override val config: VAFIndexConfig

    /** Store for the [Marks]. */
    private val marksStore: Atomic.Var<Marks> = this.store.atomicVar(VAF_INDEX_MARKS_FIELD, Marks.Serializer).createOrOpen()

    /** Store for the signatures. */
    private val signatures = this.store.indexTreeList(VAF_INDEX_SIGNATURES_FIELD, VAFSignature.Serializer).createOrOpen()

    init {
        require(this.columns.size == 1) { "$VAFIndex only supports indexing a single column." }

        /* Load or create config. */
        val configOnDisk = this.store.atomicVar(INDEX_CONFIG_FIELD, VAFIndexConfig.Serializer).createOrOpen()
        if (configOnDisk.get() == null) {
            if (config != null) {
                this.config = config
            } else {
                this.config = VAFIndexConfig(50)
            }
            configOnDisk.set(this.config)
        } else {
            this.config = configOnDisk.get()
        }
        this.store.commit()
    }

    /** False since [VAFIndex] currently doesn't support incremental updates. */
    override val supportsIncrementalUpdate: Boolean = false

    /** True since [VAFIndex] supports partitioning. */
    override val supportsPartitioning: Boolean = true

    /**
     * Calculates the cost estimate if this [VAFIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate) =
        if (predicate is KnnPredicate && predicate.column == this.columns[0] && (predicate.distance is MinkowskiDistance || predicate.distance is SquaredEuclidianDistance)) {
            Cost(
                this.signatures.size * this.marksStore.get().d * Cost.COST_DISK_ACCESS_READ + 0.1f * (this.signatures.size * this.columns[0].type.physicalSize * Cost.COST_DISK_ACCESS_READ),
                this.signatures.size * this.marksStore.get().d * (2 * Cost.COST_MEMORY_ACCESS + Cost.COST_FLOP) + 0.1f * this.signatures.size * predicate.atomicCpuCost,
                predicate.k * this.produces.map { it.type.physicalSize }.sum().toFloat()
            )
        } else {
            Cost.INVALID
        }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [VAFIndex].
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate) = predicate is KnnPredicate && predicate.column == this.columns[0]

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [VAFIndex].
     *
     * @param context The [TransactionContext] to create this [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * A [IndexTx] that affects this [AbstractIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {
        /**
         * Returns the number of [VAFSignature]s in this [VAFIndex]
         *
         * @return The number of [VAFSignature] stored in this [VAFIndex]
         */
        override fun count(): Long = this.withReadLock {
            this@VAFIndex.signatures.size.toLong()
        }

        /**
         * (Re-)builds the [VAFIndex] from scratch.
         */
        override fun rebuild() = this.withWriteLock {
            LOGGER.debug("Rebuilding VAF index {}", this@VAFIndex.name)

            /* Prepare transaction for entity. */
            val txn = this.context.getTx(this@VAFIndex.parent) as EntityTx

            /* Obtain minimum and maximum per dimension. */
            val stat = this.dbo.parent.statistics[this@VAFIndex.columns[0]]
            val min = DoubleArray(this@VAFIndex.columns[0].type.logicalSize)
            val max = DoubleArray(this@VAFIndex.columns[0].type.logicalSize)
            when (stat) {
                is FloatVectorValueStatistics -> repeat(min.size) {
                    min[it] = stat.min.data[it].toDouble()
                    max[it] = stat.max.data[it].toDouble()
                }
                is DoubleVectorValueStatistics -> repeat(min.size) {
                    min[it] = stat.min.data[it]
                    max[it] = stat.max.data[it]
                }
                is IntVectorValueStatistics -> repeat(min.size) {
                    min[it] = stat.min.data[it].toDouble()
                    max[it] = stat.max.data[it].toDouble()
                }
                is LongVectorValueStatistics -> repeat(min.size) {
                    min[it] = stat.min.data[it].toDouble()
                    max[it] = stat.max.data[it].toDouble()
                }
                else -> {
                    /* Brute force :-( This may take a while. */
                    txn.scan(this@VAFIndex.columns).forEach { r ->
                        val value = r[this@VAFIndex.columns[0]] as VectorValue<*>
                        for (i in 0 until value.logicalSize) {
                            min[i] = min(min[i], value[i].asDouble().value)
                            max[i] = max(max[i], value[i].asDouble().value)
                        }
                    }
                }
            }

            /* Calculate and update marks. */
            val marks = MarksGenerator.getEquidistantMarks(min, max, IntArray(this@VAFIndex.columns[0].type.logicalSize) { this@VAFIndex.config.marksPerDimension })
            this@VAFIndex.marksStore.set(marks)

            /* Calculate and update signatures. */
            this@VAFIndex.signatures.clear()
            txn.scan(this@VAFIndex.columns).forEach { r ->
                val value = r[this@VAFIndex.columns[0]]
                if (value is RealVectorValue<*>) {
                    this@VAFIndex.signatures.add(VAFSignature(r.tupleId, marks.getCells(value)))
                }
            }

            this@VAFIndex.dirtyField.compareAndSet(true, false)
            LOGGER.debug("Done rebuilding VAF index {}", this@VAFIndex.name)
        }

        /**
         * Updates the [VAFIndex] with the provided [DataChangeEvent]s. Since the [VAFIndex] does
         * not support incremental updates, calling this method will simply set the [VAFIndex]
         * [dirty] flag to true.
         *
         * @param event [DataChangeEvent]s to process.
         */
        override fun update(event: DataChangeEvent) = this.withWriteLock {
            this@VAFIndex.dirtyField.compareAndSet(false, true)
            Unit
        }

        /**
         * Clears the [VAFIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.withWriteLock {
            this@VAFIndex.dirtyField.compareAndSet(false, true)
            this@VAFIndex.signatures.clear()
        }

        /**
         * Performs a lookup through this [VAFIndex.Tx] and returns a [Iterator] of all [Record]s
         * that match the [Predicate]. Only supports [KnnPredicate]s.
         *
         * <strong>Important:</strong> The [Iterator] is not thread safe! It remains to the
         * caller to close the [Iterator]
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate) = filterRange(predicate, 0, 1)

        /**
         * Performs a lookup through this [VAFIndex.Tx] and returns a [Iterator] of all [Record]s
         * that match the [Predicate] within the given [LongRange]. Only supports [KnnPredicate]s.
         *
         * <strong>Important:</strong> The [Iterator] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partitionIndex The [partitionIndex] for this [filterRange] call.
         * @param partitions The total number of partitions for this [filterRange] call.
         * @return The resulting [Iterator].
         */
        override fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int) = object : Iterator<Record> {

            /** Cast  to [KnnPredicate] (if such a cast is possible).  */
            private val predicate = if (predicate is KnnPredicate) {
                predicate
            } else {
                throw QueryException.UnsupportedPredicateException("Index '${this@VAFIndex.name}' (VAF Index) does not support predicates of type '${predicate::class.simpleName}'.")
            }

            /** [VectorValue] used for query. Must be prepared before using the [Iterator]. */
            private val query: RealVectorValue<*>

            /** The [Marks] used by this [Iterator]. */
            private val marks = this@VAFIndex.marksStore.get()

            /** The [Bounds] objects used for filtering. */
            private val bounds: Bounds

            /** The [ArrayDeque] of [StandaloneRecord] produced by this [VAFIndex]. Evaluated lazily! */
            private val resultsQueue: ArrayDeque<StandaloneRecord> by lazy { prepareResults() }

            /** The [IntRange] that should be scanned by this [VAFIndex]. */
            private val range: IntRange

            init {
                this@Tx.withReadLock { }
                val value = this.predicate.query.value
                check(value is RealVectorValue<*>) { "Bound value for query vector has wrong type (found = ${value.type})." }
                this.query = value
                this.bounds = when (this.predicate.distance) {
                    is ManhattanDistance -> L1Bounds(this.query, this.marks)
                    is EuclidianDistance -> L2Bounds(this.query, this.marks)
                    is SquaredEuclidianDistance -> L2SBounds(this.query, this.marks)
                    is MinkowskiDistance -> LpBounds(this.query, this.marks, this.predicate.distance.p)
                    else -> throw IllegalArgumentException("The ${this.predicate.distance} distance kernel is not supported by VAFIndex.")
                }

                /* Calculate partition size. */
                val pSize = floorDiv(this@VAFIndex.signatures.size, partitions) + 1
                this.range = pSize * partitionIndex until min(pSize * (partitionIndex + 1), this@VAFIndex.signatures.size)
            }

            override fun hasNext(): Boolean = this.resultsQueue.isNotEmpty()

            override fun next(): Record = this.resultsQueue.removeFirst()

            /**
             * Executes the kNN and prepares the results to return by this [Iterator].
             */
            private fun prepareResults(): ArrayDeque<StandaloneRecord> {

                /* Prepare txn and kNN data structures. */
                val txn = this@Tx.context.getTx(this@VAFIndex.parent) as EntityTx
                val knn = if (this.predicate.k == 1) {
                    MinSingleSelection<ComparablePair<Long, DoubleValue>>()
                } else {
                    MinHeapSelection(this.predicate.k)
                }

                /* Iterate over all signatures. */
                var read = 0L
                for (sigIndex in this.range) {
                    val signature = this@VAFIndex.signatures[sigIndex]
                    if (signature != null) {
                        if (knn.size < this.predicate.k || this.bounds.isVASSACandidate(signature, knn.peek()!!.second.value)) {
                            val value = txn.read(signature.tupleId, this@VAFIndex.columns)[this@VAFIndex.columns[0]]
                            if (value is VectorValue<*>) {
                                knn.offer(ComparablePair(signature.tupleId, this.predicate.distance(value, this.query)))
                            }
                            read += 1
                        }
                    }
                }

                val skipped = ((1.0 - (read.toDouble() / (this@VAFIndex.signatures.size))) * 100)
                LOGGER.debug("VA-file scan: Skipped over $skipped% of entries.")

                /* Prepare and return list of results. */
                val queue = ArrayDeque<StandaloneRecord>(this.predicate.k)
                for (i in 0 until knn.size) {
                    queue.add(StandaloneRecord(knn[i].first, this@VAFIndex.produces, arrayOf(knn[i].second)))
                }
                return queue
            }
        }
    }
}