package org.vitrivr.cottontail.database.queries.predicates.bool

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.model.basics.Record
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
 * @version 2.0.0
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
     * Returns the matching score, if the provided [Record]. Score of 0.0 equates to a non-match,
     * while 1.0 equates to a full match.
     *
     * @param record The [Record] that should be checked against the predicate.
     */
    abstract fun score(record: Record): Double

    /**
     * Executes late [Value] binding using the given [BindingContext].
     *
     * @param ctx [BindingContext] used to resolve [Binding]s.
     */
    abstract override fun bindValues(ctx: BindingContext<Value>): BooleanPredicate

    /**
     * An atomic [BooleanPredicate] that compares the column of a [Record] to a provided value, a set of provided values or another column.
     *
     * @author Ralph Gasser
     * @version 1.1.0
     */
    sealed class Atomic(val left: ColumnDef<*>, val operator: ComparisonOperator, val not: Boolean) : BooleanPredicate() {

        /** The number of operations required by this [Atomic]. */
        override val atomicCpuCost: Float
            get() = this.operator.atomicCpuCost

        /** The [Atomic]s that make up this [BooleanPredicate]. */
        override val atomics: Set<Atomic>
            get() = setOf(this)

        /**
         * Checks if the provided [Record] matches this [Atomic] and assigns a score if so.
         *
         * @param record The [Record] to check.
         * @return Matching score.
         */
        override fun score(record: Record): Double = if (this.matches(record)) {
            1.0
        } else {
            0.0
        }

        /**
         * Calculates and returns the digest for this [BooleanPredicate.Atomic]
         *
         * @return Digest as [Long]
         */
        override fun digest(): Long = 33L * this.hashCode()

        /**
         * [Atomic.Literal] operator, which compares a column to (a) literal value(s).
         */
        class Literal(left: ColumnDef<*>, operator: ComparisonOperator, not: Boolean, val dependsOn: GroupId = -1) : Atomic(left, operator, not) {
            /** The number of operations required by this [Atomic]. */
            override val atomicCpuCost: Float
                get() = this.operator.atomicCpuCost

            /** Set of [ColumnDef] that are affected by this [Literal]. */
            override val columns: Set<ColumnDef<*>>
                get() = setOf(this.left)

            /**
             * Prepares this [BooleanPredicate] for use in query execution, e.g., by executing late value binding.
             *
             * @param ctx [BindingContext] to use to resolve [Binding]s.
             * @return This [BooleanPredicate.Atomic]
             */
            override fun bindValues(ctx: BindingContext<Value>): BooleanPredicate {
                this.operator.bindValues(ctx)
                return this
            }

            /**
             * Checks if the provided [Record] matches this [Atomic] and returns true or false respectively.
             *
             * @param record The [Record] to check.
             * @return true if [Record] matches this [Atomic], false otherwise.
             */
            override fun matches(record: Record): Boolean =
                (!this.not && this.operator.match(record[this.left])) || (this.not && !this.operator.match(record[this.left]))

            /**
             * Generates a [String] representation of this [BooleanPredicate].
             *
             * @return [String]
             */
            override fun toString(): String {
                val builder = StringBuilder()
                if (this.not) builder.append("!(")
                builder.append(this.left.name.toString())
                builder.append(" ")
                builder.append(this.operator.toString())
                if (this.not) builder.append(")")
                return builder.toString()
            }

            override fun equals(other: Any?): Boolean {
                if (this === other) return true
                if (other !is Literal) return false

                if (left != other.left) return false
                if (operator != other.operator) return false
                if (not != other.not) return false

                return true
            }

            override fun hashCode(): Int {
                var result = left.hashCode()
                result = 31 * result + operator.hashCode()
                result = 31 * result + not.hashCode()
                return result
            }
        }

        /**
         * [Atomic.Reference] operator, which compares a column to another column.
         *
         * Only [ComparisonOperator.Binary] operators are allowed for these types of comparisons.
         */
        class Reference(left: ColumnDef<*>, val right: ColumnDef<*>, val binaryOperator: ComparisonOperator.Binary, not: Boolean) : Atomic(left, binaryOperator, not) {

            /** [Atomic.Reference] use their own, local [BindingContext]. */
            private val context = BindingContext<Value>()

            /** Set of [ColumnDef] that are affected by this [Literal]. */
            override val columns: Set<ColumnDef<*>>
                get() = setOf(this.left, this.right)

            init {
                this.context.register(this.binaryOperator.right, null)
            }

            /**
             * Checks if the provided [Record] matches this [Atomic.Reference] and returns true or false respectively.
             *
             * @param record The [Record] to check.
             * @return true if [Record] matches this [Atomic.Reference], false otherwise.
             */
            override fun matches(record: Record): Boolean {
                this.context.update(this.binaryOperator.right, record[this.right])
                return (!this.not && this.binaryOperator.match(record[this.left])) || (this.not && !this.binaryOperator.match(record[this.left]))
            }

            /**
             * Value binding has no effect on [BooleanPredicate.Atomic.Reference]
             */
            override fun bindValues(ctx: BindingContext<Value>): BooleanPredicate {
                this.binaryOperator.bindValues(this.context)
                return this
            }

            /**
             * Generates a [String] representation of this [BooleanPredicate].
             *
             * @return [String]
             */
            override fun toString(): String {
                val builder = StringBuilder()
                if (this.not) builder.append("!(")
                builder.append(this.left.name.toString())
                builder.append(" ")
                builder.append(this.operator)
                if (this.not) builder.append(")")
                return builder.toString()
            }

            override fun equals(other: Any?): Boolean {
                if (this === other) return true
                if (other !is Reference) return false

                if (left != other.left) return false
                if (right != other.right) return false
                if (operator != other.operator) return false
                if (not != other.not) return false

                return true
            }

            override fun hashCode(): Int {
                var result = left.hashCode()
                result = 31 * result + right.hashCode()
                result = 31 * result + operator.hashCode()
                result = 31 * result + not.hashCode()
                return result
            }
        }
    }

    /**
     * A compound [BooleanPredicate] that connects two other [BooleanPredicate]s through a logical
     * AND or OR connection.
     *
     * @author Ralph Gasser
     * @version 1.1.0
     */
    class Compound(val connector: ConnectionOperator, val p1: BooleanPredicate, val p2: BooleanPredicate) : BooleanPredicate() {

        /** The total number of operations required by this [Compound]. */
        override val atomicCpuCost
            get() = this.p1.atomicCpuCost + this.p2.atomicCpuCost

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

        /**
         * Checks if the provided [Record] matches this [Compound] and returns a score.
         *
         * @param record The [Record] to check.
         * @return true if [Record] matches this [Atomic], false otherwise.
         */
        override fun score(record: Record): Double = when {
            this.connector == ConnectionOperator.AND && p1.matches(record) && p2.matches(record) -> 1.0
            this.connector == ConnectionOperator.OR -> ((p1.score(record) + p2.score(record)) / 2.0)
            else -> 0.0
        }

        override fun toString(): String = "$p1 $connector $p2"

        /**
         * Binds [Value] from the [BindingContext] to this [BooleanPredicate.Compound].
         *
         * @param ctx [BindingContext] to use to resolve this [Binding]s.
         * @return This [BooleanPredicate.Compound]
         */
        override fun bindValues(ctx: BindingContext<Value>): BooleanPredicate {
            this.p1.bindValues(ctx)
            this.p2.bindValues(ctx)
            return this
        }

        /**
         * Calculates and returns the digest for this [BooleanPredicate.Compound]
         *
         * @return Digest as [Long]
         */
        override fun digest(): Long = 33L * this.hashCode()

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Compound) return false

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