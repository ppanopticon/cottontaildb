package org.vitrivr.cottontail.database.queries.predicates.bool

import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.ValueBinding
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.values.pattern.LikePatternValue
import org.vitrivr.cottontail.model.values.pattern.LucenePatternValue
import org.vitrivr.cottontail.model.values.types.Value
import java.util.*

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
     * Executes late [Value] binding using the given [QueryContext].
     *
     * @param context [QueryContext]  used to resolve [ValueBinding]s.
     */
    abstract override fun bindValues(context: QueryContext): BooleanPredicate

    /**
     * Calculates and returns the digest of this [BooleanPredicate].
     *
     * @return Digest of this [BooleanPredicate] as [Long]
     */
    override fun digest(): Long = this.javaClass.hashCode().toLong()

    /**
     * An atomic [BooleanPredicate] that compares the column of a [Record] to a provided value (or a set of provided values).
     *
     * @author Ralph Gasser
     * @version 1.1.0
     */
    data class Atomic(
        val column: ColumnDef<*>,
        val operator: ComparisonOperator,
        val not: Boolean
    ) : BooleanPredicate() {
        /** [ValueBinding]s associated with this [BooleanPredicate.Atomic]. */
        var values: MutableCollection<Value> = ObjectLinkedOpenHashSet()
            private set

        /** [Value]s associated with this [BooleanPredicate.Atomic]. */
        private val bindings = LinkedList<ValueBinding>()

        /** The number of operations required by this [Atomic]. */
        override val cost: Float
            get() = when (this.operator) {
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
         * Adds a [ValueBinding] to this [Predicate],
         *
         * @param value The [ValueBinding] to add.
         * @return this
         */
        fun value(value: ValueBinding): Atomic {
            this.bindings.add(value)
            return this
        }

        /**
         * Adds a [Value] to this [Predicate],
         *
         * @param value The [Value] to add.
         * @return this
         */
        fun value(value: Value): Atomic {
            if (this.operator == ComparisonOperator.LIKE && value !is LikePatternValue) throw IllegalArgumentException(
                "Comparison operator of type ${this.operator} requires a LikePatternValue as right operand."
            )
            if (this.operator == ComparisonOperator.MATCH && value !is LucenePatternValue) throw IllegalArgumentException(
                "Comparison operator of type ${this.operator} requires a LucenePatternValue as right operand."
            )
            this.values.add(value)
            return this
        }

        /**
         * Clears all [values] and [bindings] in this [BooleanPredicate.Atomic].
         *
         * @return this
         */
        fun clear(): Atomic {
            this.values.clear()
            this.bindings.clear()
            return this
        }

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

        /**
         * Prepares this [BooleanPredicate] for use in query execution, e.g., by executing late value binding.
         *
         * @param context [QueryContext] to use to resolve [ValueBinding]s.
         * @return This [BooleanPredicate.Atomic]
         */
        override fun bindValues(context: QueryContext): BooleanPredicate {
            if (!this.bindings.isEmpty()) {
                this.values.clear()
                this.bindings.forEach {
                    val value = it.bind(context)
                        ?: throw IllegalStateException("Failed to bind value for value binding $it.")
                    this.value(value)
                }
            }
            return this
        }

        override fun toString(): String {
            val builder = StringBuilder()
            if (this.not) builder.append("!(")
            builder.append(this.column.name.toString())
            builder.append(" ")
            builder.append(this.operator)
            if (!this.values.isEmpty()) {
                builder.append(" [")
                builder.append(this.values.joinToString(","))
                builder.append("]")
            } else if (!this.bindings.isEmpty()) {
                builder.append(" [")
                builder.append(this.bindings.joinToString(","))
                builder.append("]")
            }
            if (this.not) builder.append(")")
            return builder.toString()
        }

        /**
         * Calculates and returns the digest for this [BooleanPredicate.Atomic]
         *
         * @return Digest as [Long]
         */
        override fun digest(): Long {
            var result = super.digest()
            result = 31L * result + this.column.hashCode()
            result = 31L * result + this.operator.hashCode()
            result = 31L * result + this.not.hashCode()
            result = 31L * result + this.bindings.hashCode()
            return result
        }
    }

    /**
     * A compound [BooleanPredicate] that connects two other [BooleanPredicate]s through a logical
     * AND or OR connection.
     *
     * @author Ralph Gasser
     * @version 1.1.0
     */
    data class Compound(val connector: ConnectionOperator, val p1: BooleanPredicate, val p2: BooleanPredicate) : BooleanPredicate() {

        /** The total number of operations required by this [Compound]. */
        override val cost
            get() = this.p1.cost + this.p2.cost

        /** The [Atomic]s that make up this [Compound]. */
        override val atomics
            get() = this.p1.atomics + this.p2.atomics

        /** Set of [ColumnDef] that are affected by this [Compound]. */
        override val columns: Set<ColumnDef<*>>
            get() =  this.p1.columns + this.p2.columns

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

        /**
         * Binds [Value] from the [QueryContext] to this [BooleanPredicate.Compound].
         *
         * @param context [QueryContext] to use to resolve this [Binding].
         * @return This [BooleanPredicate.Compound]
         */
        override fun bindValues(context: QueryContext): Compound {
            this.p1.bindValues(context)
            this.p2.bindValues(context)
            return this
        }

        /**
         * Calculates and returns the digest for this [BooleanPredicate.Compound]
         *
         * @return Digest as [Long]
         */
        override fun digest(): Long {
            var result = super.digest()
            result = 31L * result + this.p1.digest()
            result = 31L * result + this.p2.digest()
            result = 31L * result + this.connector.hashCode()
            return result
        }
    }
}