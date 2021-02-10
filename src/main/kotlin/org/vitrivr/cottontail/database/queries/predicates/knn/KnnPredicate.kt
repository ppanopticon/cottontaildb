package org.vitrivr.cottontail.database.queries.predicates.knn

import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.math.knn.metrics.DistanceKernel
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A k nearest neighbour (kNN) lookup [Predicate]. It can be used to compare the distance between
 * database [Record] and given a query vector and select the closes k entries.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
data class KnnPredicate(
    val column: ColumnDef<*>,
    val k: Int,
    val distance: DistanceKernel,
    val query: List<VectorValue<*>>,
    val weights: List<VectorValue<*>>? = null,
    val hint: KnnPredicateHint? = null
) : Predicate {

    init {
        /* Some basic sanity checks. */
        if (k <= 0) throw QueryException.QuerySyntaxException("The value of k for a kNN query cannot be smaller than one (is $k)s!")
        query.forEach {
            if (column.type.logicalSize != it.logicalSize) throw QueryException.QueryBindException("The size of the provided column ${column.name} (s_c=${column.type.logicalSize}) does not match the size of the query vector (s_q=${it.logicalSize}).")
        }
        weights?.forEach {
            if (column.type.logicalSize != it.logicalSize) throw QueryException.QueryBindException("The size of the provided column ${column.name} (s_c=${column.type.logicalSize}) does not match the size of the weight vector (s_w=${it.logicalSize}).")
        }
    }

    /** Columns affected by this [KnnPredicate]. */
    override val columns: Set<ColumnDef<*>>
        get() = setOf(this.column)

    /** Cost required for applying this [KnnPredicate] to a single record. */
    override val cost: Float =
        this.distance.costForDimension(this.query.first().logicalSize) * (this.query.size + (this.weights?.size
            ?: 0))

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as KnnPredicate

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