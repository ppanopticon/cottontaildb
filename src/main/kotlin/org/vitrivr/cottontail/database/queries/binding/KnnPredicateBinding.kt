package org.vitrivr.cottontail.database.queries.binding

import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicateHint
import org.vitrivr.cottontail.math.knn.metrics.DistanceKernel
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A [Binding] for a [KnnPredicate].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class KnnPredicateBinding(
    val column: ColumnDef<*>,
    val k: Int,
    val distance: DistanceKernel,
    val query: List<ValueBinding>,
    val weights: List<ValueBinding>? = null,
    val hint: KnnPredicateHint? = null
) : Binding<KnnPredicate>, Predicate {

    /** Columns affected by the [KnnPredicate] represented by this [KnnPredicateBinding]  */
    override val columns: Set<ColumnDef<*>> = setOf(this.column)

    /** An estimation of the CPU [Cost] required to apply the [KnnPredicate] represented by this [KnnPredicateBinding] to a single record. */
    override val cost: Float
        get() = this.distance.costForDimension(this.query.first().type.logicalSize) * (this.query.size + (this.weights?.size
            ?: 0))

    /**
     * Returns value [KnnPredicate] for this [Binding] given the [QueryContext].
     *
     * @param context [QueryContext] to use to resolve this [Binding].
     * @return [KnnPredicate]
     */
    override fun apply(context: QueryContext): KnnPredicate = KnnPredicate(
        this.column,
        this.k,
        this.distance,
        this.query.map { it.apply(context) }.filterIsInstance(VectorValue::class.java),
        this.weights?.map { it.apply(context) }?.filterIsInstance(VectorValue::class.java),
        this.hint
    )

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as KnnPredicateBinding

        if (column != other.column) return false
        if (k != other.k) return false
        if (distance != other.distance) return false
        if (query != other.query) return false
        if (weights != other.weights) return false
        if (hint != other.hint) return false

        return true
    }

    override fun hashCode(): Int {
        var result = column.hashCode()
        result = 31 * result + k
        result = 31 * result + distance.hashCode()
        result = 31 * result + query.hashCode()
        result = 31 * result + (weights?.hashCode() ?: 0)
        result = 31 * result + (hint?.hashCode() ?: 0)
        return result
    }
}