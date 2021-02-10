package org.vitrivr.cottontail.database.index.gg

import org.mapdb.DB
import org.mapdb.HTreeMap
import org.mapdb.Serializer
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.index.pq.PQIndex
import org.vitrivr.cottontail.database.index.va.VAFIndex
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.math.knn.metrics.Distances
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import org.vitrivr.cottontail.math.knn.selection.MinSingleSelection
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.extensions.write
import org.vitrivr.cottontail.utilities.math.KnnUtilities
import java.nio.file.Path
import java.util.*
import kotlin.collections.ArrayDeque

/**
 * An index structure for nearest neighbour search (NNS) based on fast grouping algorithm proposed in [1].
 * Can be used for for all types of [VectorValue]s (real and complex) as well as [Distances]. However,
 * the index must be built and prepared for a specific [Distances] metric.
 *
 * The algorithm is based on Fast Group Matching proposed in [1].
 *
 * References:
 * [1] Cauley, Stephen F., et al. "Fast group matching for MR fingerprinting reconstruction." Magnetic resonance in medicine 74.2 (2015): 523-528.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.0.0
 */
class GGIndex(
    override val name: Name.IndexName,
    override val parent: Entity,
    override val columns: Array<ColumnDef<*>>,
    override val path: Path,
    config: GGIndexConfig? = null
) : Index() {
    companion object {
        const val CONFIG_NAME = "gg_config"
        const val GG_INDEX_NAME = "gg_means"
        const val GG_INDEX_DIRTY = "gg_dirty"
        val LOGGER = LoggerFactory.getLogger(GGIndex::class.java)!!
    }

    /** The [PQIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = arrayOf(
        KnnUtilities.queryIndexColumnDef(this.parent.name),
        KnnUtilities.distanceColumnDef(this.parent.name)
    )

    /** The type of [Index]. */
    override val type = IndexType.GG

    /** The [GGIndexConfig] used by this [PQIndex] instance. */
    private val config: GGIndexConfig

    /** The internal [DB] reference. */
    private val db: DB = this.parent.parent.parent.config.mapdb.db(this.path)

    /** Store of the groups mean vector and the associated [TupleId]s. */
    private val groupsStore: HTreeMap<VectorValue<*>, LongArray> = this.db.hashMap(
        GG_INDEX_NAME,
        this.columns[0].type.serializer() as Serializer<VectorValue<*>>,
        Serializer.LONG_ARRAY
    ).counterEnable().createOrOpen()

    /** Internal storage variable for the dirty flag. */
    private val dirtyStore = this.db.atomicBoolean(GG_INDEX_DIRTY).createOrOpen()

    /**
     * Flag indicating if this [PQIndex] has been closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    /** False since [GGIndex] currently doesn't support incremental updates. */
    override val supportsIncrementalUpdate: Boolean = false

    /** True since [GGIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    /** Always false, due to incremental updating being supported. */
    override val dirty: Boolean
        get() = this.dirtyStore.get()

    init {
        require(this.columns.size == 1) { "GGIndex only supports indexing a single column." }

        /* Load or create config. */
        val configOnDisk = this.db.atomicVar(CONFIG_NAME, GGIndexConfig.Serializer).createOrOpen()
        if (configOnDisk.get() == null) {
            if (config != null) {
                this.config = config
            } else {
                this.config = GGIndexConfig(50, System.currentTimeMillis(), Distances.L2)
            }
            configOnDisk.set(this.config)
        } else {
            this.config = configOnDisk.get()
        }
        this.db.commit()
    }

    /**
     * Checks if this [Index] can process the provided [Predicate] and returns true if so and false otherwise.
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate) = predicate is KnnPredicate
            && predicate.columns.first() == this.columns[0]
            && predicate.distance == this.config.distance.kernel

    /**
     * Calculates the cost estimate if this [Index] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate) = Cost.ZERO // todo...

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [GGIndex].
     *
     * @param context The [TransactionContext] to create this [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * Closes this [GGIndex] and the associated data structures.
     */
    override fun close() = this.closeLock.write {
        if (!this.closed) {
            this.db.close()
            this.closed = true
        }
    }

    /**
     * A [IndexTx] that affects this [Index].
     */
    private inner class Tx(context: TransactionContext) : Index.Tx(context) {
        /**
         * Returns the number of groups in this [GGIndex]
         *
         * @return The number of groups stored in this [GGIndex]
         */
        override fun count(): Long = this.withReadLock {
            this@GGIndex.groupsStore.size.toLong()
        }

        /**
         * Rebuilds the surrounding [PQIndex] from scratch using the following, greedy grouping algorithm:
         *
         *  # Takes one dictionary element (random is probably easiest to start with)
         *  # Go through all yet ungrouped elements and find k = groupSize = numElementsTotal/numGroups most similar ones
         *  # Build mean vector of those k in the group and store as group representation
         *  # Don't do any PCA/SVD as we only have 18-25 ish dims...
         *  # Repeat with a new randomly selected element from the remaining ones until no elements remain.
         *
         *  Takes around 6h for 5000 groups on 9M vectors
         */
        override fun rebuild() = this.withWriteLock {

            /* Obtain some learning data for training. */
            PQIndex.LOGGER.debug("Rebuilding GG index {}", this@GGIndex.name)

            /* Load all tuple ids into a set. */
            val txn = this.context.getTx(this.dbo.parent) as EntityTx
            val remainingTids = mutableSetOf<Long>()
            txn.scan(emptyArray()).forEach { remainingTids.add(it.tupleId) }

            /* Prepare necessary data structures. */
            val groupSize =
                ((remainingTids.size + config.numGroups - 1) / config.numGroups)  // ceildiv
            val finishedTIds = mutableSetOf<Long>()
            val random = SplittableRandom(this@GGIndex.config.seed)
            val kernel = this@GGIndex.config.distance.kernel

            /* Start rebuilding the index. */
            this@GGIndex.groupsStore.clear()
            while (remainingTids.isNotEmpty()) {
                /* Randomly pick group seed value. */
                val groupSeedTid = remainingTids.elementAt(random.nextInt(remainingTids.size))
                val groupSeedValue =
                    txn.read(groupSeedTid, this@GGIndex.columns)[this@GGIndex.columns[0]]
                if (groupSeedValue is VectorValue<*>) {
                    /* Perform kNN for group. */
                    val knn =
                        MinHeapSelection<ComparablePair<Pair<TupleId, VectorValue<*>>, DoubleValue>>(
                            groupSize
                        )
                    remainingTids.forEach { tid ->
                        val r = txn.read(tid, this@GGIndex.columns)
                        val vec = r[this@GGIndex.columns[0]]
                        if (vec is VectorValue<*>) {
                            val distance = kernel.invoke(vec, groupSeedValue)
                            if (knn.size < groupSize || knn.peek()!!.second > distance) {
                                knn.offer(ComparablePair(Pair(tid, vec), distance))
                            }
                        }
                    }

                    var groupMean = groupSeedValue.new()
                    val groupTids = mutableListOf<Long>()
                    for (i in 0 until knn.size) {
                        val element = knn[i].first
                        groupMean += element.second
                        groupTids.add(element.first)
                        check(remainingTids.remove(element.first)) { "${name.simple} processed an element that should have been removed by now." }
                        check(finishedTIds.add(element.first)) { "${name.simple} processed an element that was already processed." }
                    }
                    groupMean /= DoubleValue(knn.size)
                    this@GGIndex.groupsStore[groupMean] = groupTids.toLongArray()
                }
            }
            this@GGIndex.dirtyStore.compareAndSet(true, false)
            PQIndex.LOGGER.debug("Rebuilding GGIndex {} complete.", this@GGIndex.name)
        }

        /**
         * Updates the [GGIndex] with the provided [DataChangeEvent]s. This method determines,
         * whether the [Record] affected by the [DataChangeEvent] should be added or updated
         *
         * @param event [DataChangeEvent]s to process.
         */
        override fun update(event: DataChangeEvent) = this.withWriteLock {
            this@GGIndex.dirtyStore.compareAndSet(false, true)
            Unit
        }

        /**
         * Performs a lookup through this [PQIndex.Tx] and returns a [CloseableIterator] of all [Record]s
         * that match the [Predicate]. Only supports [KnnPredicate]s.
         *
         * <strong>Important:</strong> The [CloseableIterator] is not thread safe! It remains to the
         * caller to close the [CloseableIterator]
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [CloseableIterator]
         */
        override fun filter(predicate: Predicate): CloseableIterator<Record> =
            object : CloseableIterator<Record> {

                /** Cast [KnnPredicate] (if such a cast is possible).  */
                private val predicate =
                    if (predicate is KnnPredicate && predicate.distance == this@GGIndex.config.distance.kernel) {
                        predicate
                    } else {
                        throw QueryException.UnsupportedPredicateException("Index '${this@GGIndex.name}' (GGIndex) does not support predicates of type '${predicate::class.simpleName}'.")
                    }

                /** The [ArrayDeque] of [StandaloneRecord] produced by this [VAFIndex]. Evaluated lazily! */
                private val resultsQueue: ArrayDeque<StandaloneRecord> by lazy {
                    prepareResults()
                }

                /** Flag indicating whether this [CloseableIterator] has been closed. */
                @Volatile
                private var closed = false

                init {
                    this@Tx.withReadLock { }
                }

                override fun hasNext(): Boolean = this.resultsQueue.isNotEmpty()

                override fun next(): Record = this.resultsQueue.removeFirst()

                override fun close() {
                    if (!this.closed) {
                        this.resultsQueue.clear()
                        this.closed = true
                    }
                }

                /**
                 * Executes the kNN and prepares the results to return by this [CloseableIterator].
                 */
                private fun prepareResults(): ArrayDeque<StandaloneRecord> {
                    /* Scan >= 10% of entries by default */
                    val considerNumGroups = (this@GGIndex.config.numGroups + 9) / 10
                    val txn = this@Tx.context.getTx(this@GGIndex.parent) as EntityTx
                    val kernel = this@GGIndex.config.distance.kernel

                    /** Phase 1): Perform kNN on the groups. */
                    require(this.predicate.k < txn.maxTupleId() / config.numGroups * considerNumGroups) { "Value of k is too large for this index considering $considerNumGroups groups." }
                    val groupKnns = this.predicate.query.map { _ ->
                        MinHeapSelection<ComparablePair<LongArray, DoubleValue>>(considerNumGroups)
                    }

                    LOGGER.debug("Scanning group mean signals.")
                    for ((queryIndex, query) in this.predicate.query.withIndex()) {
                        this@GGIndex.groupsStore.forEach {
                            groupKnns[queryIndex].offer(
                                ComparablePair(
                                    it.value,
                                    kernel.invoke(it.key, query)
                                )
                            )
                        }
                    }

                    /** Phase 2): Perform kNN on the per-group results. */
                    val knns = if (this.predicate.k == 1) {
                        this.predicate.query.map { MinSingleSelection<ComparablePair<Long, DoubleValue>>() }
                    } else {
                        this.predicate.query.map {
                            MinHeapSelection<ComparablePair<Long, DoubleValue>>(
                                this.predicate.k
                            )
                        }
                    }
                    LOGGER.debug("Scanning group members.")
                    for ((queryIndex, query) in this.predicate.query.withIndex()) {
                        val knn = knns[queryIndex]
                        val gknn = groupKnns[queryIndex]
                        for (k in 0 until gknn.size) {
                            for (tupleId in gknn[k].first) {
                                val value =
                                    txn.read(tupleId, this@GGIndex.columns)[this@GGIndex.columns[0]]
                                if (value is VectorValue<*>) {
                                    val distance = kernel.invoke(value, query)
                                    if (knn.size < knn.k || knn.peek()!!.second > distance) {
                                        knn.offer(ComparablePair(tupleId, distance))
                                    }
                                }
                            }
                        }
                    }

                    /* Phase 3: Prepare and return list of results. */
                    val queue =
                        ArrayDeque<StandaloneRecord>(this.predicate.k * this.predicate.query.size)
                    for ((queryIndex, knn) in knns.withIndex()) {
                        for (i in 0 until knn.size) {
                            queue.add(
                                StandaloneRecord(
                                    knn[i].first,
                                    this@GGIndex.produces,
                                    arrayOf(IntValue(queryIndex), knn[i].second)
                                )
                            )
                        }
                    }
                    return queue
                }
            }

        /**
         * Range filtering is not supported [GGIndex]
         *
         * @param predicate The [Predicate] for the lookup
         * @param range The [LongRange] of [GGIndex] to consider.
         * @return The resulting [CloseableIterator]
         */
        override fun filterRange(
            predicate: Predicate,
            range: LongRange
        ): CloseableIterator<Record> {
            throw UnsupportedOperationException("The UniqueHashIndex does not support ranged filtering!")
        }

        /**
         * Commits changes to the [GGIndex].
         */
        override fun performCommit() {
            this@GGIndex.db.commit()
        }

        /**
         * Makes a rollback on all changes to the [GGIndex].
         */
        override fun performRollback() {
            this@GGIndex.db.rollback()
        }
    }
}