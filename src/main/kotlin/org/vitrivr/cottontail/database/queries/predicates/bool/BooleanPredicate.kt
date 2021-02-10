package org.vitrivr.cottontail.database.queries.predicates.bool

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.values.pattern.LikePatternValue
import org.vitrivr.cottontail.model.values.pattern.LucenePatternValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [Predicate] that can be used to match a [Record]s using boolean [ComparisonOperator]s and [ConnectionOperator]s.
 *
 * A [BooleanPredicate] either matches a [Record] or not, returning true or false respectively.
 * All types of [BooleanPredicate] are constructed using conjunctive normal form (CNF).
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class BooleanPredicate : Predicate {
    /** The [Atomic]s that make up this [BooleanPredicate]. */
    abstract val atomics: Set<Atomic>

    /**
     * Returns true, if the provided [Record] matches the [Predicate] and false otherwise.
     *
     * @param record The [Record] that should be checked against the predicate.
     */
    abstract fun matches(record: Record): Boolean

    /**
     * An atomic [BooleanPredicate] that compares the column of a [Record] to a provided value (or a set of provided values).
     *
     * @author Ralph Gasser
     * @version 1.1.0
     */
    data class Atomic(
        private val column: ColumnDef<*>,
        val operator: ComparisonOperator,
        val not: Boolean = false,
        var values: Collection<Value>
    ) : BooleanPredicate() {
        init {
            /* Optimization; uses a set for a IN operation. */
            if (this.operator == ComparisonOperator.IN) {
                this.values = ObjectOpenHashSet(this.values)
            }

            /* Optimization: Converts the incoming StringValue to a LikePatternValue. */
            if (this.operator == ComparisonOperator.LIKE) {
                if (!this.values.all { it is LikePatternValue }) {
                    throw IllegalArgumentException("Comparison operator of type ${this.operator} requires a LikePatternValue as right operand.")
                }
            }

            /* Optimization: Converts the incoming StringValue to a LucenePatternValue. */
            if (this.operator == ComparisonOperator.MATCH) {
                if (!this.values.all { it is LucenePatternValue }) {
                    throw IllegalArgumentException("Comparison operator of type ${this.operator} requires a LikePatternValue as right operand.")
                }
            }
        }

        /** The number of operations required by this [Atomic]. */
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

        /** Set of [ColumnDef] that are affected by this [Atomic]. */
        override val columns: Set<ColumnDef<*>>
            get() = setOf(this.column)

        /** The [Atomic]s that make up this [BooleanPredicate]. */
        override val atomics: Set<Atomic>
            get() = setOf(this)

        /**
         * Checks if the provided [Record] matches this [Atomic] and returns true or false respectively.
         *
         * @param record The [Record] to check.
         * @return true if [Record] matches this [Atomic], false otherwise.
         */
        override fun matches(record: Record): Boolean = if (this.not) {
            !this.operator.match(record[this.column], this.values)
        } else {
            this.operator.match(record[this.column], this.values)
        }

        override fun toString(): String = if (this.not) {
            "!(${this.column.name} ${this.operator} :values[${this.values.size}]"
        } else {
            "(${this.column.name} ${this.operator} :values[${this.values.size}]"
        }
    }

    /**
     * A compound [BooleanPredicate] that connects two other [BooleanPredicate]s through a logical
     * AND or OR connection.
     *
     * @author Ralph Gasser
     * @version 1.1.0
     */
    data class Compound(
        val connector: ConnectionOperator,
        val p1: BooleanPredicate,
        val p2: BooleanPredicate
    ) : BooleanPredicate() {

        /** The total number of operations required by this [Compound]. */
        override val cost = this.p1.cost + this.p2.cost

        /** The [Atomic]s that make up this [Compound]. */
        override val atomics
            get() = this.p1.atomics + this.p2.atomics

        /** Set of [ColumnDef] that are affected by this [Compound]. */
        override val columns: Set<ColumnDef<*>>
            get() = this.p1.columns + this.p2.columns

        /**
         * Checks if the provided [Record] matches this [Compound] and returns true or false respectively.
         *
         * @param record The [Record] to check.
         * @return true if [Record] matches this [Compound], false otherwise.
         */
        override fun matches(record: Record): Boolean = when (connector) {
            ConnectionOperator.AND -> this.p1.matches(record) && this.p2.matches(record)
            ConnectionOperator.OR -> this.p1.matches(record) || this.p2.matches(record)
        }

        override fun toString(): String = "$p1 $connector $p2"
    }
}