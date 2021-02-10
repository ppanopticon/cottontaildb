package org.vitrivr.cottontail.database.queries.binding

import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.database.queries.predicates.bool.ConnectionOperator
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [Binding] for a [BooleanPredicate].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class BooleanPredicateBinding : Binding<BooleanPredicate>, Predicate {

    /** The [Atomic]s that make up this [BooleanPredicate]. */
    abstract val atomics: Set<Atomic>

    /**
     * A [Binding] for a [BooleanPredicate.Atomic].
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    data class Atomic(
        private val column: ColumnDef<*>,
        val operator: ComparisonOperator,
        val not: Boolean = false,
        var values: Collection<ValueBinding>
    ) : BooleanPredicateBinding() {

        /**
         * The number of operations required by the [BooleanPredicate.Atomic]
         * represented by this [Atomic].
         */
        override val cost: Float = when (this.operator) {
            ComparisonOperator.ISNULL,
            ComparisonOperator.ISNOTNULL -> 1.0f
            ComparisonOperator.EQUAL,
            ComparisonOperator.GREATER,
            ComparisonOperator.LESS,
            ComparisonOperator.GEQUAL,
            ComparisonOperator.LEQUAL -> 2.0f
            ComparisonOperator.BETWEEN -> 4.0f
            ComparisonOperator.IN -> this.values.size + 1.0f
            ComparisonOperator.LIKE -> 10.0f /* ToDo: Make more explicit. */
            ComparisonOperator.MATCH -> 10.0f
        }

        /**
         * Set of [ColumnDef] that are affected by this [BooleanPredicate.Atomic]
         * represented by this [Atomic].
         */
        override val columns: Set<ColumnDef<*>>
            get() = setOf(this.column)

        /** The [Atomic]s that make up this [BooleanPredicate]. */
        override val atomics: Set<Atomic>
            get() = setOf(this)

        /**
         * Returns [BooleanPredicate.Atomic] for this [Atomic]
         * given the [QueryContext].
         *
         * @param context [QueryContext] to use to resolve this [Binding].
         * @return [BooleanPredicate.Atomic]
         */
        override fun apply(context: QueryContext): BooleanPredicate = BooleanPredicate.Atomic(
            this.column,
            this.operator,
            this.not,
            this.values.map {
                it.apply(context)
                    ?: throw IllegalStateException("NULL values are not explicitly allowed in boolean predicates.")
            })

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Atomic

            if (column != other.column) return false
            if (operator != other.operator) return false
            if (not != other.not) return false
            if (values != other.values) return false

            return true
        }

        override fun hashCode(): Int {
            var result = column.hashCode()
            result = 31 * result + operator.hashCode()
            result = 31 * result + not.hashCode()
            result = 31 * result + values.hashCode()
            return result
        }
    }

    /**
     * A [Binding] for a [BooleanPredicate.Compound].
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    data class Compound(
        val connector: ConnectionOperator,
        val p1: BooleanPredicateBinding,
        val p2: BooleanPredicateBinding
    ) : BooleanPredicateBinding() {
        /** The total number of operations required by this [Compound]. */
        override val cost = this.p1.cost + this.p2.cost

        /** The [Atomic]s that make up this [Compound]. */
        override val atomics
            get() = this.p1.atomics + this.p2.atomics

        /** Set of [ColumnDef] that are affected by this [Compound]. */
        override val columns: Set<ColumnDef<*>>
            get() = this.p1.columns + this.p2.columns

        /**
         * Returns [BooleanPredicate.Compound] for this
         * [Compound] given the [QueryContext].
         *
         * @param context [QueryContext] to use to resolve this [Binding].
         * @return [BooleanPredicate.Compound]
         */
        override fun apply(context: QueryContext): BooleanPredicate = BooleanPredicate.Compound(
            this.connector,
            this.p1.apply(context),
            this.p2.apply(context)
        )

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Compound

            if (connector != other.connector) return false
            if (p1 != other.p1) return false
            if (p2 != other.p2) return false

            return true
        }

        override fun hashCode(): Int {
            var result = connector.hashCode()
            result = 31 * result + p1.hashCode()
            result = 31 * result + p2.hashCode()
            return result
        }
    }
}