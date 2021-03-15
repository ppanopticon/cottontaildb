package org.vitrivr.cottontail.database.queries.predicates.bool

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.pattern.LikePatternValue
import org.vitrivr.cottontail.model.values.types.Value
import java.util.*


/**
 * The sealed [ComparisonOperator]s class.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class ComparisonOperator {

    /** The atomic CPU cost of matching this [ComparisonOperator] with a [Value]. */
    abstract val atomicCpuCost: Float

    /**
     * Matches the given [Value] to this [ComparisonOperator] and returns true,
     * if there is a match (i.e. value satisfies operation) and false otherwise.
     *
     * @return True on match, false otherwise.
     */
    abstract fun match(left: Value?): Boolean

    /**
     * Binds the values in this [ComparisonOperator] to this [BindingContext].
     *
     * @param ctx The [BindingContext] to bind the values to.
     */
    abstract fun bindValues(ctx: BindingContext<Value>)

    /**
     * A [ComparisonOperator] that checks if a value is NULL.
     */
    class IsNull : ComparisonOperator() {
        override val atomicCpuCost: Float = Cost.COST_MEMORY_ACCESS
        override fun match(left: Value?) = (left == null)
        override fun bindValues(ctx: BindingContext<Value>) { /* No Op. */
        }
    }

    /**
     * A [ComparisonOperator] that expresses an equality (==) comparison.
     */
    sealed class Binary(val right: Binding<Value>) : ComparisonOperator() {

        override val atomicCpuCost: Float = 2 * Cost.COST_MEMORY_ACCESS

        /**
         * Binds the values in this [ComparisonOperator] to this [BindingContext].
         *
         * @param ctx The [BindingContext] to bind the values to.
         */
        override fun bindValues(ctx: BindingContext<Value>) {
            this.right.context = ctx
        }

        class Equal(right: Binding<Value>) : Binary(right) {
            override fun match(left: Value?) = (left != null) && left.isEqual(this.right.value)
            override fun toString(): String = "= $right"
        }

        /**
         * A [ComparisonOperator] that expresses greater (>) comparison.
         */
        class Greater(right: Binding<Value>) : Binary(right) {
            override fun match(left: Value?): Boolean {
                return (left != null) && left > this.right.value
            }

            override fun toString(): String = "> $right"

        }

        /**
         * A [ComparisonOperator] that expresses less (<) comparison.
         */
        class Less(right: Binding<Value>) : Binary(right) {
            override fun match(left: Value?) = (left != null) && left < this.right.value
            override fun toString(): String = "< $right"
        }

        /**
         * A [ComparisonOperator] that expresses greater or equal (>=) comparison.
         */
        class GreaterEqual(right: Binding<Value>) : Binary(right) {
            override fun match(left: Value?) = (left != null) && left >= this.right.value
            override fun toString(): String = ">= $right"
        }

        /**
         * A [ComparisonOperator] that expresses less or equal (<=) comparison.
         */
        class LessEqual(right: Binding<Value>) : Binary(right) {
            override fun match(left: Value?) = (left != null) && left <= this.right.value
            override fun toString(): String = "<= $right"
        }

        /**
         * A [ComparisonOperator] that expresses a LIKE comparison. I.e. left LIKE right.
         */
        class Like(right: Binding<Value>) : Binary(right) {
            override fun match(left: Value?) = (left is StringValue) && (this.right.value as LikePatternValue).matches(left)
            override fun toString(): String = "LIKE $right"
        }

        /**
         * A [ComparisonOperator] that expresses a MATCH comparison. Can only be evaluated through a lucene index.
         */
        class Match(right: Binding<Value>) : Binary(right) {
            override fun match(left: Value?) = throw UnsupportedOperationException("A MATCH comparison operator cannot be evaluated directly.")
            override fun toString(): String = "MATCH $right"
        }
    }

    /**
     * A [ComparisonOperator] that expresses a BETWEEN comparison (i.e. lower <= left <= upper).
     */
    class Between(val rightLower: Binding<Value>, val rightUpper: Binding<Value>) : ComparisonOperator() {
        override val atomicCpuCost: Float = 4.0f * Cost.COST_MEMORY_ACCESS
        override fun match(left: Value?) = (left != null) && left in this.rightLower.value..this.rightUpper.value
        override fun bindValues(ctx: BindingContext<Value>) {
            this.rightLower.context = ctx
            this.rightUpper.context = ctx
        }

        override fun toString(): String = "BETWEEN $rightLower, $rightUpper"
    }

    /**
     * A [ComparisonOperator] that expresses a IN comparison (i.e. left IN right).
     */
    class In(right: MutableList<Binding<Value>>) : ComparisonOperator() {
        private val rightBindings = LinkedList<Binding<Value>>()
        private val rightValues = ObjectOpenHashSet<Value>()
        val right: List<Binding<Value>> = Collections.unmodifiableList(this.rightBindings)
        override val atomicCpuCost: Float = 4.0f * Cost.COST_MEMORY_ACCESS
        override fun match(left: Value?): Boolean = left in this.rightValues

        init {
            right.forEach { this.addBinding(it) }
        }

        /**
         * Adds a [Binding] to this [In] operator.
         *
         * @param binding [Binding] to add.
         */
        fun addBinding(binding: Binding<Value>) {
            this.rightBindings.add(binding)
            try {
                this.rightValues.add(binding.value)
            } catch (e: IllegalStateException) {
                /* Ignore. */
            }
        }

        /**
         * Removes all [Binding]s from this [In] operator.
         */
        fun clear() {
            this.rightBindings.clear()
            this.rightValues.clear()
        }

        /**
         * Binds the [Binding]s in this [In] operator to the given [BindingContext].
         * Updates all [Value]s accordingly.
         *
         * @param ctx The [BindingContext]
         */
        override fun bindValues(ctx: BindingContext<Value>) {
            this.rightValues.clear()
            this.rightBindings.forEach {
                it.context = ctx
                this.rightValues.add(it.value)
            }
        }

        override fun toString(): String = "IN [${this.rightBindings.joinToString(",")}]"
    }
}