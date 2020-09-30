package org.vitrivr.cottontail.database.queries.components

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.KnnPredicateHint
import org.vitrivr.cottontail.math.knn.metrics.DistanceKernel
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.values.PatternValue
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A general purpose [Predicate] that describes a Cottontail DB query. It can either operate on
 * [Recordset][org.vitrivr.cottontail.model.recordset.Recordset]s or data read from an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1
 */
sealed class Predicate {
    /** An estimation of the [Cost] required to apply this [Predicate] to a [Record]. */
    abstract val cost: Float

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
sealed class BooleanPredicate : Predicate() {
    /** The [AtomicBooleanPredicate]s that make up this [BooleanPredicate]. */
    abstract val atomics: Set<AtomicBooleanPredicate<*>>

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
data class AtomicBooleanPredicate<T : Value>(private val column: ColumnDef<T>, val operator: ComparisonOperator, val not: Boolean = false, var values: Collection<Value>) : BooleanPredicate() {
    init {
        if (this.operator == ComparisonOperator.IN) {
            this.values = this.values.toSet()
        }

        if (this.operator == ComparisonOperator.LIKE) {
            this.values = this.values.mapNotNull {
                if (it is StringValue) {
                    PatternValue(it.value)
                } else {
                    null
                }
            }
        }
    }

    /** The number of operations required by this [AtomicBooleanPredicate]. */
    override val cost: Float = 3 * Cost.COST_MEMORY_ACCESS_READ

    /** Set of [ColumnDef] that are affected by this [AtomicBooleanPredicate]. */
    override val columns: Set<ColumnDef<T>> = setOf(this.column)

    /** The [AtomicBooleanPredicate]s that make up this [BooleanPredicate]. */
    override val atomics: Set<AtomicBooleanPredicate<*>>
        get() = setOf(this)

    /**
     * Checks if the provided [Record] matches this [AtomicBooleanPredicate] and returns true or false respectively.
     *
     * @param record The [Record] to check.
     * @return true if [Record] matches this [AtomicBooleanPredicate], false otherwise.
     */
    override fun matches(record: Record): Boolean {
        require(record.has(this.column)) { "AtomicBooleanPredicate cannot be applied to record because it does not contain the expected column ${this.column}." }
        return if (this.not) {
            !this.operator.match(record[this.column], this.values)
        } else {
            this.operator.match(record[this.column], this.values)
        }
    }
}

/**
 * A compound [BooleanPredicate] that connects two other [BooleanPredicate]s through a logical AND or OR connection.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
data class CompoundBooleanPredicate(val connector: ConnectionOperator, val p1: BooleanPredicate, val p2: BooleanPredicate) : BooleanPredicate() {
    /** The [AtomicBooleanPredicate]s that make up this [CompoundBooleanPredicate]. */
    override val atomics = this.p1.atomics + this.p2.atomics

    /** Set of [ColumnDef] that are affected by this [CompoundBooleanPredicate]. */
    override val columns: Set<ColumnDef<*>> = p1.columns + p2.columns

    /** The total number of operations required by this [CompoundBooleanPredicate]. */
    override val cost = this.p1.cost + this.p2.cost

    /**
     * Checks if the provided [Record] matches this [CompoundBooleanPredicate] and returns true or false respectively.
     *
     * @param record The [Record] to check.
     * @return true if [Record] matches this [CompoundBooleanPredicate], false otherwise.
     */
    override fun matches(record: Record): Boolean = when (connector) {
        ConnectionOperator.AND -> p1.matches(record) && p2.matches(record)
        ConnectionOperator.OR -> p1.matches(record) || p2.matches(record)
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
data class KnnPredicate<T : VectorValue<*>>(val column: ColumnDef<T>, val k: Int, val query: List<T>, val distance: DistanceKernel, val weights: List<VectorValue<*>>? = null, val hint: KnnPredicateHint? = null) : Predicate() {
    init {
        /* Some basic sanity checks. */
        if (k <= 0) throw QueryException.QuerySyntaxException("The value of k for a kNN query cannot be smaller than one (is $k)s!")
        query.forEach {
            if (column.logicalSize != it.logicalSize) throw QueryException.QueryBindException("The size of the provided column ${column.name} (s_c=${column.logicalSize}) does not match the size of the query vector (s_q=${it.logicalSize}).")
        }
        weights?.forEach {
            if (column.logicalSize != it.logicalSize) throw QueryException.QueryBindException("The size of the provided column ${column.name} (s_c=${column.logicalSize}) does not match the size of the weight vector (s_w=${it.logicalSize}).")
        }
    }

    /**
     * Columns affected by this [KnnPredicate].
     */
    override val columns: Set<ColumnDef<*>> = setOf(column)

    /** Cost required for applying this [KnnPredicate] to a single record. */
    override val cost: Float = Cost.COST_MEMORY_ACCESS_READ * this.distance.cost * (this.query.size + (this.weights?.size
            ?: 0))

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as KnnPredicate<*>

        if (column != other.column) return false
        if (k != other.k) return false
        if (query != other.query) return false
        if (distance != other.distance) return false
        if (weights != other.weights) return false
        if (hint != other.hint) return false
        return true
    }

    override fun hashCode(): Int {
        var result = column.hashCode()
        result = 31 * result + k
        result = 31 * result + query.hashCode()
        result = 31 * result + distance.hashCode()
        result = 31 * result + (weights?.hashCode() ?: 0)
        result = 31 * result + (hint?.hashCode() ?: 0)
        return result
    }
}