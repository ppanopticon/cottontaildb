package org.vitrivr.cottontail.database.index.lsh.superbit

import org.mapdb.HTreeMap
import org.mapdb.Serializer
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.index.lsh.LSHIndex
import org.vitrivr.cottontail.database.index.va.signature.VAFSignature
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.math.knn.metrics.AbsoluteInnerProductDistance
import org.vitrivr.cottontail.math.knn.metrics.CosineDistance
import org.vitrivr.cottontail.math.knn.metrics.RealInnerProductDistance
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.model.values.types.ComplexVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import java.nio.file.Path
import java.util.*

/**
 * Represents a LSH based index in the Cottontail DB data model. An [Index] belongs to an [Entity]
 * and can be used to index one to many [Column]s. Usually, [Index]es allow for faster data access.
 * They process [Predicate]s and return
 * [Recordset]s.
 *
 * @author Manuel Huerbin, Gabriel Zihlmann & Ralph Gasser
 * @version 1.5.0
 */
class SuperBitLSHIndex<T : VectorValue<*>>(
    name: Name.IndexName,
    parent: Entity,
    columns: Array<ColumnDef<*>>,
    path: Path,
    config: SuperBitLSHIndexConfig? = null
) : LSHIndex<T>(name, parent, columns, path) {

    companion object {
        private const val SBLSH_INDEX_CONFIG = "lsh_config"
        private const val SBLSH_INDEX_DIRTY = "lsh_dirty"

        private val LOGGER = LoggerFactory.getLogger(SuperBitLSHIndex::class.java)
    }

    /** False since [SuperBitLSHIndex] doesn't supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = false

    /** False since [SuperBitLSHIndex] doesn't support partitioning. */
    override val supportsPartitioning: Boolean = false

    /** Indicates if this [SuperBitLSHIndex] is currently considered dirty, i.e., out of sync with [Entity]. */
    override val dirty: Boolean
        get() = this.dirtyStore.get()

    /** The [IndexType] of this [SuperBitLSHIndex]. */
    override val type = IndexType.LSH_SB

    /** The [SuperBitLSHIndexConfig] used by this [SuperBitLSHIndex] instance. */
    private val config: SuperBitLSHIndexConfig

    /** The [SuperBitLSHIndexConfig] used by this [SuperBitLSHIndex] instance. */
    private val maps: List<HTreeMap<Int, LongArray>>

    /** Internal storage variable for the dirty flag. */
    private val dirtyStore = this.db.atomicBoolean(SBLSH_INDEX_DIRTY).createOrOpen()

    init {
        if (!columns.all { it.type.vector }) {
            throw DatabaseException.IndexNotSupportedException(
                name,
                "Because only vector columns are supported for SuperBitLSHIndex."
            )
        }
        val configOnDisk =
            this.db.atomicVar(SBLSH_INDEX_CONFIG, SuperBitLSHIndexConfig.Serializer).createOrOpen()
        if (configOnDisk.get() == null) {
            if (config != null) {
                this.config = config
                configOnDisk.set(config)
            } else {
                LOGGER.warn("No config supplied and the config from disk was also empty. Resorting to dummy config. Delete this index ASAP!")
                this.config = SuperBitLSHIndexConfig(1, 1, 123L, true, SamplingMethod.GAUSSIAN)
            }
        } else {
            this.config = configOnDisk.get()
        }
        this.maps = List(this.config.stages) {
            this.db.hashMap(MAP_FIELD_NAME + "_stage$it", Serializer.INTEGER, Serializer.LONG_ARRAY)
                .counterEnable().createOrOpen()
        }

        /* Initial commit to underlying DB. */
        this.db.commit()
    }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [SuperBitLSHIndex].
     * note: only use the innerproduct distances with normalized vectors!
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean =
        predicate is KnnPredicate
                && predicate.columns.first() == this.columns[0]
                && (predicate.distance is CosineDistance
                || predicate.distance is RealInnerProductDistance
                || predicate.distance is AbsoluteInnerProductDistance)
                && (!this.config.considerImaginary || predicate.query.all { it is ComplexVectorValue<*> })

    /**
     * Calculates the cost estimate of this [SuperBitLSHIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost = if (canProcess(predicate)) {
        Cost.ZERO /* TODO: Determine. */
    } else {
        Cost.INVALID
    }

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [Index].
     *
     * @param context The [TransactionContext] to create this [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * A [IndexTx] that affects this [Index].
     */
    private inner class Tx(context: TransactionContext) : Index.Tx(context) {
        /**
         * Returns the number of [VAFSignature]s in this [SuperBitLSHIndex]
         *
         * @return The number of [VAFSignature] stored in this [SuperBitLSHIndex]
         */
        override fun count(): Long = this.withReadLock {
            this@SuperBitLSHIndex.maps.map { it.count().toLong() }.sum()
        }

        /**
         * (Re-)builds the [SuperBitLSHIndex].
         */
        override fun rebuild() = this.withWriteLock {
            LOGGER.debug("Rebuilding SB-LSH index {}", this@SuperBitLSHIndex.name)

            /* LSH. */
            val tx = this.context.getTx(this.dbo.parent) as EntityTx
            val specimen = this.acquireSpecimen(tx)
                ?: throw DatabaseException("Could not gather specimen to create index.") // todo: find better exception
            val lsh = SuperBitLSH(
                this@SuperBitLSHIndex.config.stages,
                this@SuperBitLSHIndex.config.buckets,
                this@SuperBitLSHIndex.config.seed,
                specimen,
                this@SuperBitLSHIndex.config.considerImaginary,
                this@SuperBitLSHIndex.config.samplingMethod
            )


            /* Locally (Re-)create index entries and sort bucket for each stage to corresponding map. */
            val local = List(config.stages) {
                MutableList(config.buckets) { mutableListOf<Long>() }
            }

            /* for every record get bucket-signature, then iterate over stages and add tid to the list of that bucket of that stage */
            tx.scan(this@SuperBitLSHIndex.columns).forEach {
                val value = it[this.columns[0]]
                    ?: throw DatabaseException("Could not find column for entry in index $this") // todo: what if more columns? This should never happen -> need to change type and sort this out on index creation
                if (value is VectorValue<*>) {
                    val buckets = lsh.hash(value)
                    (buckets zip local).forEach { (bucket, map) ->
                        map[bucket].add(it.tupleId)
                    }
                } else {
                    throw DatabaseException("$value is no vector column!")
                }
            }

            /* Clear existing maps. */
            (this@SuperBitLSHIndex.maps zip local).forEach { (map, localdata) ->
                map.clear()
                localdata.forEachIndexed { bucket, tIds ->
                    map[bucket] = tIds.toLongArray()
                }
            }

            /* Update dirty flag. */
            this@SuperBitLSHIndex.dirtyStore.compareAndSet(true, false)
            LOGGER.debug("Rebuilding SB-LSH index completed.")
        }

        /**
         * Updates the [SuperBitLSHIndex] with the provided [DataChangeEvent]. This method determines,
         * whether the [Record] affected by the [DataChangeEvent] should be added or updated
         *
         * @param event [DataChangeEvent]s to process.
         */
        override fun update(event: DataChangeEvent) = this.withWriteLock {
            this@SuperBitLSHIndex.dirtyStore.compareAndSet(false, true)
            Unit
        }

        /**
         * Performs a lookup through this [SuperBitLSHIndex] and returns a [CloseableIterator] of
         * all [TupleId]s that match the [Predicate]. Only supports [KnnPredicate]s.
         *
         * The [CloseableIterator] is not thread safe!
         *
         * <strong>Important:</strong> It remains to the caller to close the [CloseableIterator]
         *
         * @param predicate The [Predicate] for the lookup*
         * @return The resulting [CloseableIterator]
         */
        override fun filter(predicate: Predicate) = object : CloseableIterator<Record> {

            /** Cast [KnnPredicate] (if such a cast is possible).  */
            val predicate = if (predicate is KnnPredicate) {
                predicate
            } else {
                throw QueryException.UnsupportedPredicateException("Index '${this@SuperBitLSHIndex.name}' (LSH Index) does not support predicates of type '${predicate::class.simpleName}'.")
            }

            /** Prepare new [SuperBitLSH] data structure to calculate the bucket index. */
            private val lsh = SuperBitLSH(
                this@SuperBitLSHIndex.config.stages,
                this@SuperBitLSHIndex.config.buckets,
                this@SuperBitLSHIndex.config.seed,
                this.predicate.query.first(),
                this@SuperBitLSHIndex.config.considerImaginary,
                this@SuperBitLSHIndex.config.samplingMethod
            )

            /** Flag indicating whether this [CloseableIterator] has been closed. */
            @Volatile
            private var closed = false

            /** The index of the current query vector. */
            private var queryIndex = -1

            /** List of [TupleId]s returned by this [CloseableIterator]. */
            private val tupleIds = LinkedList<TupleId>()

            /* Performs some sanity checks. */
            init {
                if (this.predicate.columns.first() != this@SuperBitLSHIndex.columns[0] || !(this.predicate.distance is CosineDistance || this.predicate.distance is AbsoluteInnerProductDistance)) {
                    throw QueryException.UnsupportedPredicateException("Index '${this@SuperBitLSHIndex.name}' (lsh-index) does not support the provided predicate.")
                }
                this@Tx.withReadLock { /* No op. */ }
                this.prepareMatchesForNextQueryVector()
            }

            override fun hasNext(): Boolean {
                check(!this.closed) { "Illegal invocation of hasNext(): This CloseableIterator has been closed." }
                return this.tupleIds.isNotEmpty() || (this.queryIndex < this.predicate.query.size)
            }

            override fun next(): Record {
                check(!this.closed) { "Illegal invocation of next(): This CloseableIterator has been closed." }
                val ret = StandaloneRecord(
                    this.tupleIds.removeFirst(),
                    this@SuperBitLSHIndex.produces,
                    arrayOf(IntValue(this.queryIndex))
                )
                if (this.tupleIds.isEmpty()) {
                    this.prepareMatchesForNextQueryVector()
                }
                return ret
            }

            override fun close() {
                if (!this.closed) {
                    this.closed = true
                }
            }

            /**
             * Prepares the matches for the next query vector by adding all [TupleId]s to this [tupleIds] list.
             */
            private fun prepareMatchesForNextQueryVector() {
                if ((++this.queryIndex) >= this.predicate.query.size) return
                val query = this.predicate.query[this.queryIndex]
                val signature = this.lsh.hash(query)
                for (stage in signature.indices) {
                    for (tupleId in this@SuperBitLSHIndex.maps[stage].getValue(signature[stage])) {
                        this.tupleIds.offer(tupleId)
                    }
                }
            }
        }

        /**
         * The [SuperBitLSHIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] to perform the lookup.
         * @param range The [LongRange] to consider.
         * @return The resulting [CloseableIterator].
         */
        override fun filterRange(
            predicate: Predicate,
            range: LongRange
        ): CloseableIterator<Record> {
            throw UnsupportedOperationException("The SuperBitLSHIndex does not support ranged filtering!")
        }

        /** Performs the actual COMMIT operation by rolling back the [IndexTx]. */
        override fun performCommit() {
            this@SuperBitLSHIndex.db.commit()
        }

        /** Performs the actual ROLLBACK operation by rolling back the [IndexTx]. */
        override fun performRollback() {
            this@SuperBitLSHIndex.db.rollback()
        }

        /**
         * Tries to find a specimen of the [VectorValue] in the [Entity] underpinning this [SuperBitLSHIndex]
         *
         * @param tx [Entity.Tx] used to read from [Entity]
         * @return A specimen of the [VectorValue] that should be indexed.
         */
        private fun acquireSpecimen(tx: EntityTx): VectorValue<*>? {
            for (index in 0L until tx.maxTupleId()) {
                val read = tx.read(index, this@SuperBitLSHIndex.columns)[this.columns[0]]
                if (read is VectorValue<*>) {
                    return read
                }
            }
            return null
        }
    }
}