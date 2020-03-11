package ch.unibas.dmi.dbis.cottontail.database.index.vaf

import ch.unibas.dmi.dbis.cottontail.database.column.Column
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnType
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.events.DataChangeEvent
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.index.IndexType
import ch.unibas.dmi.dbis.cottontail.database.queries.KnnPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import org.mapdb.DBMaker
import org.mapdb.HTreeMap
import org.mapdb.Serializer
import org.slf4j.LoggerFactory
import java.nio.file.Path

/**
 * Represents a VAF based index in the Cottontail DB data model. An [Index] belongs to an [Entity] and can be used to
 * index one to many [Column]s. Usually, [Index]es allow for faster data access. They process [Predicate]s and return
 * [Recordset]s.
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class VAFIndex(override val name: Name, override val parent: Entity, override val columns: Array<ColumnDef<*>>) : Index() {

    /**
     * Index-wide constants.
     */
    companion object {
        const val MAP_FIELD_NAME = "vaf_map"
        private val LOGGER = LoggerFactory.getLogger(VAFIndex::class.java)
    }

    /** Constant FQN of the [Schema] object. */
    override val fqn: Name = this.parent.fqn.append(this.name)

    /** Path to the [VAFIndex] file. */
    override val path: Path = this.parent.path.resolve("idx_vaf_$name.db")

    /** The type of [Index] */
    override val type: IndexType = IndexType.VAF

    /** The [VAFIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef(parent.fqn.append("distance"), ColumnType.forName("DOUBLE")))

    /** The internal [DB] reference. */
    private val db = if (parent.parent.parent.config.forceUnmapMappedFiles) {
        DBMaker.fileDB(this.path.toFile()).fileMmapEnable().cleanerHackEnable().transactionEnable().make()
    } else {
        DBMaker.fileDB(this.path.toFile()).fileMmapEnable().transactionEnable().make()
    }

    /** Map structure used for [VAFIndex]. */
    private val map: HTreeMap<out Int, LongArray> = this.db.hashMap(MAP_FIELD_NAME, Serializer.INTEGER, Serializer.LONG_ARRAY).counterEnable().createOrOpen()

    /**
     * Flag indicating if this [VAFIndex] has been closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    /**
     * Performs a lookup through this [VAFIndex].
     *
     * @param predicate The [Predicate] for the lookup
     * @return The resulting [Recordset]
     */
    override fun filter(predicate: Predicate, tx: Entity.Tx): Recordset = if (predicate is KnnPredicate<*>) {
        /* Guard: Only process predicates that are supported. */
        require(this.canProcess(predicate)) { throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (vaf-index) does not support the provided predicate.") }

        /* Create empty recordset. */
        val recordset = Recordset(this.produces)
        val castPredicate = predicate as KnnPredicate<*>

        /* Generate record set .*/
        // TODO

        recordset
    } else {
        throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (vaf-index) does not support predicates of type '${predicate::class.simpleName}'.")
    }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [VAFIndex].
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean = if (predicate is KnnPredicate<*>) {
        // TODO
        predicate.columns.first() == this.columns[0]
    } else {
        false
    }

    /**
     * Calculates the cost estimate of this [VAFIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Float = when {
        predicate !is KnnPredicate<*> || predicate.columns.first() != this.columns[0] -> Float.MAX_VALUE
        else -> 1f // TODO
    }

    /**
     * Returns true since [VAFIndex] supports incremental updates.
     *
     * @return True
     */
    override fun supportsIncrementalUpdate(): Boolean = true

    /**
     * (Re-)builds the [VAFIndex].
     */
    override fun rebuild(tx: Entity.Tx) {
        LOGGER.trace("rebuilding index {}", name)

        /* Clear existing map. */
        this.map.clear()

        /* (Re-)create index entries. */
        val localMap = mutableMapOf<Int, MutableList<Long>>()

        // TODO

        val castMap = this.map as HTreeMap<Int, LongArray>
        localMap.forEach { (bucket, l) -> castMap[bucket] = l.toLongArray() }
        this.db.commit()
    }

    /**
     * Updates the [VAFIndex] with the provided [Record]. This method determines, whether the [Record] should be added or updated
     *
     * @param record Record to update the [VAFIndex] with.
     */
    override fun update(update: Collection<DataChangeEvent>, tx: Entity.Tx) = try {
        // TODO
    } catch (e: Throwable) {
        this.db.rollback()
        throw e
    }

    /**
     * Closes this [VAFIndex] and the associated data structures.
     */
    override fun close() = this.globalLock.write {
        if (!this.closed) {
            this.db.close()
            this.closed = true
        }
    }

}