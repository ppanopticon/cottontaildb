package org.vitrivr.cottontail.database.queries.predicates.knn

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.ValueBinding
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.math.knn.metrics.DistanceKernel
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
import java.util.*

/**
 * A k nearest neighbour (kNN) lookup [Predicate]. It can be used to compare the distance between
 * database [Record] and given a query vector and select the closes k entries.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
open class KnnPredicate(
    val column: ColumnDef<*>,
    val k: Int,
    val distance: DistanceKernel,
    val hint: KnnPredicateHint? = null
) : Predicate {

    init {
        /* Basic sanity checks. */
        check(this.k >= 0) { }
        if (this.k <= 0) throw QueryException.QuerySyntaxException("The value of k for a kNN query cannot be smaller than one (is $k)s!")
    }

    /** List of weight [VectorValue]s. */
    private val _query: MutableList<VectorValue<*>> = LinkedList()
    val weights: List<VectorValue<*>>
        get() = Collections.unmodifiableList(this._weights)

    /** List of query [VectorValue]s. */
    private val _weights: MutableList<VectorValue<*>> = LinkedList()
    val query: List<VectorValue<*>>
        get() = Collections.unmodifiableList(this._query)

    /** List of [ValueBinding] for [query] vectors. */
    private val _queryBindings = LinkedList<ValueBinding>()

    /** List of [ValueBinding] for [weights] vectors. */
    private val _weightsBindings = LinkedList<ValueBinding>()

    /** Columns affected by this [KnnPredicate]. */
    override val columns: Set<ColumnDef<*>>
        get() = setOf(this.column)

    /** Cost required for applying this [KnnPredicate] to a single record. */
    override val cost: Float
        get() = this.distance.costForDimension(this.query.first().logicalSize) * (this.query.size + this.weights.size)

    /**
     * Adds a [ValueBinding] to this [KnnPredicate],
     *
     * @param value The [ValueBinding] to add.
     * @return this
     */
    fun query(value: ValueBinding): KnnPredicate {
        require(value.type != this.column.type) { "The provided query vector does not match the kNN column ${column.name} (type = ${column.type})." }
        this._queryBindings.add(value)
        return this
    }

    /**
     * Adds a query [VectorValue] to this [KnnPredicate],
     *
     * @param value The [VectorValue] to add.
     * @return this
     */
    fun query(value: VectorValue<*>): KnnPredicate {
        require(value.type != this.column.type) { "The provided query vector does not match the kNN column ${column.name} (type = ${column.type})." }
        this._query.add(value)
        return this
    }

    /**
     * Adds a [ValueBinding] to this [KnnPredicate],
     *
     * @param value The [ValueBinding] to add.
     * @return this
     */
    fun weight(value: ValueBinding): KnnPredicate {
        require(value.type != this.column.type) { "The provided weight vector does not match the kNN column ${column.name} (type = ${column.type})." }
        this._weightsBindings.add(value)
        return this
    }

    /**
     * Adds a weight [VectorValue] to this [KnnPredicate],
     *
     * @param value The [VectorValue] to add.
     * @return this
     */
    fun weight(value: VectorValue<*>): KnnPredicate {
        require(value.type != this.column.type) { "The provided weight vector does not match the kNN column ${column.name} (type = ${column.type})." }
        this._weights.add(value)
        return this
    }

    /**
     * Clears all [Value]s and [ValueBinding]s in this [KnnPredicate].
     *
     * @return this
     */
    fun clear(): KnnPredicate {
        this._query.clear()
        this._queryBindings.clear()
        this._weights.clear()
        this._weightsBindings.clear()
        return this
    }

    /**
     * Prepares this [KnnPredicate] for use in query execution, e.g., by executing late value binding.
     *
     * @param context [QueryContext] to use to resolve [ValueBinding]s.
     * @return this [KnnPredicate]
     */
    override fun bindValues(context: QueryContext): KnnPredicate {
        if (!this._queryBindings.isEmpty()) {
            this._query.clear()
            this._queryBindings.forEach {
                val value = it.bind(context)
                if (value is VectorValue<*>) {
                    this._query.add(value)
                } else {
                    throw IllegalStateException("Failed to bind value for value binding $it.")
                }
            }
        }
        if (!this._weightsBindings.isEmpty()) {
            this._weightsBindings.clear()
            this._weightsBindings.forEach {
                val value = it.bind(context)
                if (value is VectorValue<*>) {
                    this._weights.add(value)
                } else {
                    throw IllegalStateException("Failed to bind value for value binding $it.")
                }
            }
        }
        return this
    }

    /**
     * Calculates and returns the digest for this [KnnPredicate]
     *
     * @return Digest of this [KnnPredicate] as [Long]
     */
    override fun digest(): Long {
        var result = this.javaClass.hashCode().toLong()
        result = 31L * result + this.column.hashCode()
        result = 31L * result + this.k.hashCode()
        result = 31L * result + this.distance.hashCode()
        result = 31L * result + this.hint.hashCode()
        result = 31L * result + this._queryBindings.hashCode()
        result = 31L * result + this._weightsBindings.hashCode()
        return result
    }

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
        result = 31 * result + weights.hashCode()
        result = 31 * result + (hint?.hashCode() ?: 0)
        return result
    }
}