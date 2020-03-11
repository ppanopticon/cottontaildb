package ch.unibas.dmi.dbis.cottontail.database.index.lsh.superbit

import ch.unibas.dmi.dbis.cottontail.database.column.Column
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.events.DataChangeEvent
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.index.lsh.LSHIndex
import ch.unibas.dmi.dbis.cottontail.database.queries.KnnPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import ch.unibas.dmi.dbis.cottontail.math.knn.ComparablePair
import ch.unibas.dmi.dbis.cottontail.math.knn.HeapSelect
import ch.unibas.dmi.dbis.cottontail.math.knn.metrics.CosineDistance
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.DoubleValue
import ch.unibas.dmi.dbis.cottontail.model.values.types.VectorValue
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name

/**
 * Represents a LSH based index in the Cottontail DB data model. An [Index] belongs to an [Entity] and can be used to
 * index one to many [Column]s. Usually, [Index]es allow for faster data access. They process [Predicate]s and return
 * [Recordset]s.
 *
 * @author Manuel Huerbin & Ralph Gasser
 * @version 1.1
 */
class SuperBitLSHIndex<T : VectorValue<*>>(name: Name, parent: Entity, columns: Array<ColumnDef<*>>, params: Map<String, String>? = null) : LSHIndex<T>(name, parent, columns) {

    companion object {
        const val CONFIG_NAME = "lsh_config"
        const val CONFIG_NAME_STAGES = "stages"
        const val CONFIG_NAME_BUCKETS = "buckets"
        const val CONFIG_NAME_SEED = "seed"
    }

    /** Internal configuration object for [SuperBitLSHIndex]. */
    val config = this.db.atomicVar(CONFIG_NAME, SuperBitLSHIndexConfig.Serializer).createOrOpen()

    init {
        if (params != null) {
            this.config.set(SuperBitLSHIndexConfig(params.getValue(CONFIG_NAME_STAGES).toInt(), params.getValue(CONFIG_NAME_BUCKETS).toInt(), params.getValue(CONFIG_NAME_SEED).toLong()))
        }
    }

    /**
     * Performs a lookup through this [SuperBitLSHIndex].
     *
     * @param predicate The [Predicate] for the lookup
     * @return The resulting [Recordset]
     */
    override fun filter(predicate: Predicate, tx: Entity.Tx): Recordset {
        if (predicate is KnnPredicate<*>) {
            /* Guard: Only process predicates that are supported. */
            require(this.canProcess(predicate)) { throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lsh-index) does not support the provided predicate.") }

            /* Prepare empty Recordset and LSH object. */
            val recordset = Recordset(this.produces, (predicate.k * predicate.query.size).toLong())
            val lsh = SuperBitLSH(this.config.get().stages, this.config.get().buckets, this.columns.first().size, this.config.get().seed, predicate.query.first())

            /* Generate record set .*/
            for (i in predicate.query.indices) {
                val query = predicate.query[i]
                val knn = HeapSelect<ComparablePair<Long, Double>>(predicate.k)
                val bucket: Int = lsh.hash(query).last()
                val tupleIds = this.map[bucket]
                if (tupleIds != null) {
                    tx.readMany(tupleIds = tupleIds.toList()).forEach {
                        val value = it[predicate.column]
                        if (value is VectorValue<*>) {
                            if (predicate.weights != null) {
                                knn.add(ComparablePair(it.tupleId, predicate.distance(query, value, predicate.weights[i])))
                            } else {
                                knn.add(ComparablePair(it.tupleId, predicate.distance(query, value)))
                            }
                        }
                    }
                    for (j in 0 until knn.size) {
                        recordset.addRowUnsafe(knn[j].first, arrayOf(DoubleValue(knn[j].second)))
                    }
                }
            }
            return recordset
        } else {
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (LSH Index) does not support predicates of type '${predicate::class.simpleName}'.")
        }
    }

    /** (Re-)builds the [SuperBitLSHIndex]. */
    override fun rebuild(tx: Entity.Tx) {
        /* LSH. */
        val specimen = this.acquireSpecimen(tx) ?: return
        val lsh = SuperBitLSH(this.config.get().stages, this.config.get().buckets, this.columns[0].size, this.config.get().seed, specimen)

        /* (Re-)create index entries locally. */
        val local = Array(this.config.get().buckets) { mutableListOf<Long>() }
        tx.forEach {
            val value = it[this.columns[0]]
            if (value is VectorValue<*>) {
                val bucket: Int = lsh.hash(value).last()
                local[bucket].add(it.tupleId)
            }
        }

        /* Replace existing map. */
        this.map.clear()
        local.forEachIndexed { bucket, list -> this.map[bucket] = list.toLongArray() }

        /* Commit local database. */
        this.db.commit()
    }

    /**
     * Updates the [SuperBitLSHIndex] with the provided [Record]. This method determines, whether the [Record] should be added or updated
     *
     * @param record Record to update the [SuperBitLSHIndex] with.
     */
    override fun update(update: Collection<DataChangeEvent>, tx: Entity.Tx) = try {
        // TODO
    } catch (e: Throwable) {
        this.db.rollback()
        throw e
    }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [SuperBitLSHIndex].
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean = if (predicate is KnnPredicate<*>) {
        predicate.columns.first() == this.columns[0] && predicate.distance is CosineDistance
    } else {
        false
    }

    /**
     * Calculates the cost estimate of this [SuperBitLSHIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Float = if (canProcess(predicate)) {
        1.0f
    } else {
        Float.MAX_VALUE
    }

    /**
     * Returns true since [SuperBitLSHIndex] supports incremental updates.
     *
     * @return True
     */
    override fun supportsIncrementalUpdate(): Boolean = true

    /**
     * Tries to find a specimen of the [VectorValue] in the [Entity] underpinning this [SuperBitLSHIndex]
     *
     * @param tx [Entity.Tx] used to read from [Entity]
     * @return A specimen of the [VectorValue] that should be indexed.
     */
    private fun acquireSpecimen(tx: Entity.Tx): VectorValue<*>? {
        for (index in 1L until tx.count()) {
            val read = tx.read(index)[this.columns[0]]
            if (read is VectorValue<*>) {
                return read
            }
        }
        return null
    }
}