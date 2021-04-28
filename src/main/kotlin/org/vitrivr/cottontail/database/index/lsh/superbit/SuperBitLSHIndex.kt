package org.vitrivr.cottontail.database.index.lsh.superbit

import org.mapdb.HTreeMap
import org.mapdb.Serializer
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.index.AbstractIndex
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.index.lsh.LSHIndex
import org.vitrivr.cottontail.database.index.va.signature.VAFSignature
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.math.knn.kernels.Distances
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.ComplexVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import java.nio.file.Path
import java.util.*

/**
 * Represents a LSH based index in the Cottontail DB data model. An [AbstractIndex] belongs to an [DefaultEntity]
 * and can be used to index one to many [Column]s. Usually, [AbstractIndex]es allow for faster data access.
 * They process [Predicate]s and return
 * [Recordset]s.
 *
 * @author Manuel Huerbin, Gabriel Zihlmann & Ralph Gasser
 * @version 2.1.0
 */
class SuperBitLSHIndex<T : VectorValue<*>>(
    path: Path,
    parent: DefaultEntity,
    config: SuperBitLSHIndexConfig? = null
) : LSHIndex<T>(path, parent) {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(SuperBitLSHIndex::class.java)
        private val SUPPORTED_DISTANCES = arrayOf(Distances.COSINE, Distances.INNERPRODUCT)
    }

    /** False since [SuperBitLSHIndex] doesn't supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = false

    /** False since [SuperBitLSHIndex] doesn't support partitioning. */
    override val supportsPartitioning: Boolean = false

    /** The [IndexType] of this [SuperBitLSHIndex]. */
    override val type = IndexType.LSH_SB

    /** The [SuperBitLSHIndexConfig] used by this [SuperBitLSHIndex] instance. */
    override val config: SuperBitLSHIndexConfig

    /** The [SuperBitLSHIndexConfig] used by this [SuperBitLSHIndex] instance. */
    private val maps: List<HTreeMap<Int, LongArray>>

    init {
        if (!columns.all { it.type.vector }) {
            throw DatabaseException.IndexNotSupportedException(
                name,
                "Because only vector columns are supported for SuperBitLSHIndex."
            )
        }
        val configOnDisk =
            this.store.atomicVar(INDEX_CONFIG_FIELD, SuperBitLSHIndexConfig.Serializer)
                .createOrOpen()
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
            this.store.hashMap(
                LSH_MAP_FIELD + "_stage$it",
                Serializer.INTEGER,
                Serializer.LONG_ARRAY
            )
                .counterEnable().createOrOpen()
        }

        /* Initial commit to underlying DB. */
        this.store.commit()
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
                && predicate.distance in SUPPORTED_DISTANCES
                && (!this.config.considerImaginary || predicate.query is ComplexVectorValue<*>)

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
     * Opens and returns a new [IndexTx] object that can be used to interact with this [AbstractIndex].
     *
     * @param context The [TransactionContext] to create this [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * A [IndexTx] that affects this [AbstractIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {
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
            val lsh = SuperBitLSH(this@SuperBitLSHIndex.config.stages, this@SuperBitLSHIndex.config.buckets, this@SuperBitLSHIndex.config.seed, specimen, this@SuperBitLSHIndex.config.considerImaginary, this@SuperBitLSHIndex.config.samplingMethod)


            /* Locally (Re-)create index entries and sort bucket for each stage to corresponding map. */
            val local = List(config.stages) {
                MutableList(config.buckets) { mutableListOf<Long>() }
            }

            /* for every record get bucket-signature, then iterate over stages and add tid to the list of that bucket of that stage */
            tx.scan(this@SuperBitLSHIndex.columns).forEach {
                val value = it[this.dbo.columns[0]] ?: throw DatabaseException("Could not find column for entry in index $this") // todo: what if more columns? This should never happen -> need to change type and sort this out on index creation
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
            this@SuperBitLSHIndex.dirtyField.compareAndSet(true, false)
            LOGGER.debug("Rebuilding SB-LSH index completed.")
        }

        /**
         * Updates the [SuperBitLSHIndex] with the provided [DataChangeEvent]. This method determines,
         * whether the [Record] affected by the [DataChangeEvent] should be added or updated
         *
         * @param event [DataChangeEvent]s to process.
         */
        override fun update(event: DataChangeEvent) = this.withWriteLock {
            this@SuperBitLSHIndex.dirtyField.compareAndSet(false, true)
            Unit
        }

        /**
         * Clears the [SuperBitLSHIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.withWriteLock {
            this@SuperBitLSHIndex.dirtyField.compareAndSet(false, true)
            (this@SuperBitLSHIndex.maps).forEach { map ->
                map.clear()
            }
        }

        /**
         * Performs a lookup through this [SuperBitLSHIndex] and returns a [Iterator] of
         * all [TupleId]s that match the [Predicate]. Only supports [KnnPredicate]s.
         *
         * The [Iterator] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup*
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate) = object : Iterator<Record> {

            /** Cast [KnnPredicate] (if such a cast is possible).  */
            private val predicate = if (predicate is KnnPredicate) {
                predicate
            } else {
                throw QueryException.UnsupportedPredicateException("Index '${this@SuperBitLSHIndex.name}' (LSH Index) does not support predicates of type '${predicate::class.simpleName}'.")
            }

            /** List of [TupleId]s returned by this [Iterator]. */
            private val tupleIds: LinkedList<TupleId>

            /* Performs some sanity checks. */
            init {
                if (this.predicate.columns.first() != this@SuperBitLSHIndex.columns[0] || !(this.predicate.distance in SUPPORTED_DISTANCES)) {
                    throw QueryException.UnsupportedPredicateException("Index '${this@SuperBitLSHIndex.name}' (lsh-index) does not support the provided predicate.")
                }

                /* Assure correctness of query vector. */
                val value = this.predicate.query.value
                check(value is VectorValue<*>) { "Bound value for query vector has wrong type (found = ${value.type})." }

                /** Prepare SuperBitLSH data structure. */
                this@Tx.withReadLock { }
                val lsh = SuperBitLSH(
                    this@SuperBitLSHIndex.config.stages,
                    this@SuperBitLSHIndex.config.buckets,
                    this@SuperBitLSHIndex.config.seed,
                    value,
                    this@SuperBitLSHIndex.config.considerImaginary,
                    this@SuperBitLSHIndex.config.samplingMethod
                )

                /** Prepare list of matches. */
                this.tupleIds = LinkedList<TupleId>()
                val signature = lsh.hash(value)
                for (stage in signature.indices) {
                    for (tupleId in this@SuperBitLSHIndex.maps[stage].getValue(signature[stage])) {
                        this.tupleIds.offer(tupleId)
                    }
                }
            }

            override fun hasNext(): Boolean {
                return this.tupleIds.isNotEmpty()
            }

            override fun next(): Record = StandaloneRecord(this.tupleIds.removeFirst(), this@SuperBitLSHIndex.produces, arrayOf())

        }

        /**
         * The [SuperBitLSHIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partitionIndex The [partitionIndex] for this [filterRange] call.
         * @param partitions The total number of partitions for this [filterRange] call.
         * @return The resulting [Iterator].
         */
        override fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int): Iterator<Record> {
            throw UnsupportedOperationException("The SuperBitLSHIndex does not support ranged filtering!")
        }

        /**
         * Tries to find a specimen of the [VectorValue] in the [DefaultEntity] underpinning this [SuperBitLSHIndex]
         *
         * @param tx [DefaultEntity.Tx] used to read from [DefaultEntity]
         * @return A specimen of the [VectorValue] that should be indexed.
         */
        private fun acquireSpecimen(tx: EntityTx): VectorValue<*>? {
            for (index in 0L until tx.maxTupleId()) {
                val read = tx.read(index, this@SuperBitLSHIndex.columns)[this.dbo.columns[0]]
                if (read is VectorValue<*>) {
                    return read
                }
            }
            return null
        }
    }
}