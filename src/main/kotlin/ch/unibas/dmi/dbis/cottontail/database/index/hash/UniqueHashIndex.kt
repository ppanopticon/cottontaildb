package ch.unibas.dmi.dbis.cottontail.database.index.hash

import ch.unibas.dmi.dbis.cottontail.database.column.Column
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.index.IndexType
import ch.unibas.dmi.dbis.cottontail.database.queries.AtomicBooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.ComparisonOperator
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.exceptions.ValidationException
import ch.unibas.dmi.dbis.cottontail.model.values.Value
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import org.mapdb.DBMaker
import org.mapdb.HTreeMap
import java.nio.file.Path
import org.mapdb.Serializer
import kotlin.concurrent.write


/**
 * Represents an index in the Cottontail DB data model. An [Index] belongs to an [Entity] and can be used to index one to many
 * [Column]s. Usually, [Index]es allow for faster data access. They process [Predicate]s and return [Recordset]s.
 *
 * @see Schema
 * @see Column
 * @see Entity.Tx
 *
 * @author Ralph Gasser
 * @version 1.0f
 */
internal class UniqueHashIndex(override val name: Name, override val parent: Entity, override val columns: Array<ColumnDef<*>>) : Index() {
    /**
     * Index-wide constants.
     */
    companion object {
        const val MAP_FIELD_NAME = "map"
        const val ATOMIC_COST = 1e-6f /** Cost of a single lookup. TODO: Determine real value. */
    }

    /** Path to the [UniqueHashIndex] file. */
    override val path: Path = this.parent.path.resolve("idx_$name.db")

    /** The type of [Index] */
    override val type: IndexType = IndexType.HASH_UQ

    /** The [UniqueHashIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = this.columns

    /** The internal [DB] reference. */
    private val db = DBMaker.fileDB(this.path.toFile()).fileMmapEnableIfSupported().transactionEnable().make()

    /** Map structure used for [UniqueHashIndex]. */
    private val map: HTreeMap<out Value<out Any>, Long> = this.db.hashMap(MAP_FIELD_NAME, this.columns.first().type.serializer(this.columns.size), Serializer.LONG_PACKED).counterEnable().createOrOpen()

    /**
     * Flag indicating if this [UniqueHashIndex] has been closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    /**
     * Performs a lookup through this [UniqueHashIndex].
     *
     * @param predicate The [Predicate] for the lookup
     * @return The resulting [Recordset]
     */
    override fun filter(predicate: Predicate): Recordset = if (predicate is AtomicBooleanPredicate<*>) {
        /* Create empty recordset. */
        val recordset = Recordset(this.columns)

        /* Fetch the columns that match the predicate. */
        val results = when(predicate.operator) {
            ComparisonOperator.IN -> predicate.values.map { it to this.map[it] }.toMap()
            ComparisonOperator.EQUAL -> mapOf(Pair(predicate.values.first(),this.map[predicate.values.first()] ))
            else -> throw QueryException.IndexLookupFailedException(this.fqn, "Instance of unique hash-index does not support ${predicate.operator} comparison operators.")
        }

        /* Generate record set .*/
        if (predicate.not) {
            this.map.forEach { (value, tid) ->
                if (results.containsKey(value)) {
                    recordset.addRowUnsafe(tupleId = tid, values = arrayOf(value))
                }
            }
        } else {
            results.forEach {
                if (it.value != null) {
                    recordset.addRowUnsafe(tupleId = it.value!!, values = arrayOf(it.key))
                }
            }
        }

        recordset
    } else {
        throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (unique hash-index) does not support predicates of type '${predicate::class.simpleName}'.")
    }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [UniqueHashIndex]. [UniqueHashIndex] can be used to process IN and EQUALS
     * comparison operations on the specified column
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean = if (predicate is AtomicBooleanPredicate<*>) {
        predicate.columns.first() == this.columns[0] && (predicate.operator == ComparisonOperator.IN || predicate.operator == ComparisonOperator.EQUAL)
    } else {
        false
    }

    /**
     * Calculates the cost estimate of this [UniqueHashIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Float = when {
        predicate !is AtomicBooleanPredicate<*> || predicate.columns.first() != this.columns[0] -> Float.MAX_VALUE
        predicate.operator == ComparisonOperator.IN -> ATOMIC_COST * predicate.values.size
        predicate.operator == ComparisonOperator.IN -> ATOMIC_COST
        else -> Float.MAX_VALUE
    }

    /**
     * (Re-)builds the [UniqueHashIndex].
     */
    override fun rebuild() {
        /* Clear existing map. */
        this.map.clear()

        /* (Re-)create index entries. */
        val localMap = this.map as HTreeMap<Value<*>,Long>
        this.parent.Tx(readonly = true, columns = this.columns).begin { tx ->
            tx.forEach(2) {
                val value = it[this.columns[0]] ?: throw ValidationException.IndexUpdateException(this.fqn, "A values cannot be null for instances of unique hash-index but tid=${it.tupleId} is")
                if (!localMap.containsKey(value)) {
                    localMap[value] = it.tupleId
                } else {
                    throw ValidationException.IndexUpdateException(this.fqn, "LongValue must be unique for instances of unique hash-index but '$value' (tid=${it.tupleId}) is not !")
                }
            }
            this.db.commit()
            true
        }
    }

    /**
     * Closes this [UniqueHashIndex] and the associated data structures.
     */
    override fun close() = this.globalLock.write {
        if (!this.closed) {
            this.db.close()
            this.closed = true
        }
    }
}