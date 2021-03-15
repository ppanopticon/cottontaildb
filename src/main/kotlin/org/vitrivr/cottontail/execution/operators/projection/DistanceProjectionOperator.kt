package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.flow.*
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.math.KnnUtilities

/**
 * A [Operator.PipelineOperator] used during query execution. It calculates the distance of a specific [ColumnDef]
 * to a set of query vectors and adds a [ColumnDef] that captures that distance. Used for NNS.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class DistanceProjectionOperator(parent: Operator, val predicate: KnnPredicate) : Operator.PipelineOperator(parent) {

    /** Query [VectorValue]. Must be prepared before using the [Iterator]. */
    private val query: VectorValue<*>

    /** Optional weight [VectorValue]. Must be prepared before using the [Iterator]. */
    private val weight: VectorValue<*>?

    /** The columns produced by this [DistanceProjectionOperator]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
        *this.parent.columns,
        KnnUtilities.distanceColumnDef(this.predicate.column.name.entity())
    )

    /** The [DistanceProjectionOperator] is not a pipeline breaker. */
    override val breaker: Boolean = false

    init {
        val q = this.predicate.query.value
        check(q is VectorValue<*>) { "Bound value for query vector has wrong type (found = ${q.type})." }
        this.query = q

        val w = this.predicate.weight?.value
        if (w != null) {
            check(w is VectorValue<*>) { "Bound value for query vector has wrong type (found = ${w.type})." }
            this.weight = w
        } else {
            this.weight = null
        }
    }

    /**
     * Converts this [DistanceProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [DistanceProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        /* Obtain parent flow. */
        val parentFlow = this.parent.toFlow(context)
        val values = Array<Value?>(this@DistanceProjectionOperator.columns.size) { null }
        val knn = this.predicate

        /* Generate new flow. */
        return if (this.weight != null) {
            parentFlow.map {
                var i = 0
                val value = it[knn.column]
                val distance = if (value is VectorValue<*>) {
                    knn.distance(this.query, value, this.weight)
                } else {
                    DoubleValue.NaN
                }
                it.forEach { _, v -> values[i++] = v }
                values[i] = distance
                StandaloneRecord(it.tupleId, this.columns, values)
            }
        } else {
            parentFlow.map {
                var i = 0
                val value = it[knn.column]
                val distance = if (value is VectorValue<*>) {
                    knn.distance(this.query, value)
                } else {
                    DoubleValue.NaN
                }
                it.forEach { _, v -> values[i++] = v }
                values[i] = distance
                StandaloneRecord(it.tupleId, this.columns, values)
            }
        }
    }
}