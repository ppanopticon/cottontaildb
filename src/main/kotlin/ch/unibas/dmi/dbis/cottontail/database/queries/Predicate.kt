package ch.unibas.dmi.dbis.cottontail.database.queries

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.math.knn.metrics.Distance
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.values.Value

/**
 * A general purpose [Predicate] that describes a Cottontail DB query. It can either operate on [Recordset]s or data read from an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
sealed class Predicate {
    /** An estimation of the operations required to apply this [Predicate] to a [Record]. */
    abstract val operations: Int

    /** Set of [ColumnDef] that are affected by this [Predicate]. */
    abstract val columns: Set<ColumnDef<*>>
}

/**
 * A boolean [Predicate] that can be used to compare a [Record] to a given value.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal sealed class BooleanPredicate : Predicate() {
    /** The [AtomicBooleanPredicate]s that make up this [BooleanPredicate]. */
    abstract val atomics: Set<AtomicBooleanPredicate>

    /**
     * Returns true, if the provided [Record] matches the [Predicate] and false otherwise.
     *
     * @param record The [Record] that should be checked against the predicate.
     */
    abstract fun matches(record: Record): Boolean
}

/**
 * A atomic [BooleanPredicate] that compares the column of a [Record] to a provided value (or a set of provided values).
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal data class AtomicBooleanPredicate(private val column: ColumnDef<*>, val operator: ComparisonOperator, var values: Collection<Value<out Any>>) : BooleanPredicate() {
    init {
        if (this.operator == ComparisonOperator.IN) {
            this.values = this.values.toSet()
        }
    }

    /** The number of operations required by this [AtomicBooleanPredicate]. */
    override val operations: Int = 1

    /** Set of [ColumnDef] that are affected by this [AtomicBooleanPredicate]. */
    override val columns: Set<ColumnDef<*>> = setOf(this.column)

    /** The [AtomicBooleanPredicate]s that make up this [BooleanPredicate]. */
    override val atomics: Set<AtomicBooleanPredicate>
        get() = setOf(this)

    /**
     * Checks if the provided [Record] matches this [AtomicBooleanPredicate] and returns true or false respectively.
     *
     * @param record The [Record] to check.
     * @return true if [Record] matches this [AtomicBooleanPredicate], false otherwise.
     */
    override fun matches(record: Record): Boolean = if (record.has(column)) {
        operator.match(record[column], values)
    } else {
        throw QueryException.ColumnDoesNotExistException(column)
    }
}

/**
 * A compound [BooleanPredicate] that connects multiple [BooleanPredicate]s through a logical AND or OR connection.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
internal class CompoundBooleanPredicate(val connector: ConnectionOperator, vararg val clauses: BooleanPredicate) : BooleanPredicate() {
    /** The [AtomicBooleanPredicate]s that make up this [CompoundBooleanPredicate]. */
    override val atomics = clauses.flatMap { it.atomics }.toSet()

    /** Set of [ColumnDef] that are affected by this [CompoundBooleanPredicate]. */
    override val columns: Set<ColumnDef<*>> =  clauses.flatMap { it.columns }.toSet()

    /** The total number of operations required by this [CompoundBooleanPredicate]. */
    override val operations = clauses.map { it.operations }.sum()

    /**
     * Checks if the provided [Record] matches this [CompoundBooleanPredicate] and returns true or false respectively.
     *
     * @param record The [Record] to check.
     * @return true if [Record] matches this [CompoundBooleanPredicate], false otherwise.
     */
    override fun matches(record: Record): Boolean = when (connector) {
        ConnectionOperator.AND -> atomics.all { it.matches(record) }
        ConnectionOperator.OR -> atomics.any { it.matches(record) }
        ConnectionOperator.NOT -> atomics.none { it.matches(record) }
    }
}

/**
 * A k nearest neighbour (kNN) lookup [Predicate]. It can be used to compare the distance between database [Record] and given a query
 * vector and select the closes k entries.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal data class KnnPredicate<T : Any>(val column: ColumnDef<T>, val k: Int, val query: List<Array<Number>>, val distance: Distance, val weights: List<Array<Number>>? = null) : Predicate() {
    init {
        /* Some basic sanity checks. */
        if (k <= 0) throw QueryException.QuerySyntaxException("The value of k for a kNN query cannot be smaller than one (is $k)s!")
        query.forEach {
            if (column.size != it.size) throw QueryException.QueryBindException("The size of the provided column ${column.name} (s_c=${column.size}) does not match the size of the query vector (s_q=${query.size}).")
        }
        weights?.forEach {
            if (column.size != it.size) {
                throw QueryException.QueryBindException("The size of the provided column ${column.name} (s_c=${column.size}) does not match the size of the weight vector (s_w=${query.size}).")
            }
        }
    }

    /**
     * Columns affected by this [KnnPredicate].
     */
    override val columns: Set<ColumnDef<*>> = setOf(column)

    /**
     * Number of operations required for this [KnnPredicate]. Calculated by applying the base operations
     * for the [Distance] to each vector components.
     *
     * If weights are used, this will be added to the cost.
     */
    override val operations: Int = distance.operations * query.size + (this.weights?.size ?: 0)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as KnnPredicate<*>

        if (column != other.column) return false
        if (k != other.k) return false
        if (query != other.query) return false
        if (distance != other.distance) return false
        if (weights != null) {
            if (other.weights == null) return false
            if (weights != other.weights) return false
        } else if (other.weights != null) return false

        return true
    }

    override fun hashCode(): Int {
        var result = column.hashCode()
        result = 31 * result + k
        result = 31 * result + query.hashCode()
        result = 31 * result + distance.hashCode()
        result = 31 * result + (weights?.hashCode() ?: 0)
        return result
    }
}