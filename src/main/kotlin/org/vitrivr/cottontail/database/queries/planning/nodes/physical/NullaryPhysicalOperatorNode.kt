package org.vitrivr.cottontail.database.queries.planning.nodes.physical

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.NullaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An abstract [OperatorNode.Physical] implementation that has no input node, i.e., acts as a source.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
abstract class NullaryPhysicalOperatorNode : OperatorNode.Physical() {
    /** The arity of the [NullaryPhysicalOperatorNode] is always on. */
    final override val inputArity = 0

    /** A [NullaryPhysicalOperatorNode] is always the root of a tree and thus has the index 0. */
    final override val depth: Int = 0

    /** The [base] of a [NullaryPhysicalOperatorNode] is always itself. */
    final override val base: Collection<Physical>
        get() = listOf(this)

    /** The [totalCost] of a [NullaryPhysicalOperatorNode] is always its own [Cost]. */
    final override val totalCost: Cost
        get() = this.cost

    /** By default, a [NullaryPhysicalOperatorNode] has no specific order. */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>> = emptyArray()

    /** By default, a [NullaryPhysicalOperatorNode] does not have specific requirements. */
    override val requires: Array<ColumnDef<*>> = emptyArray()

    /**
     * Creates and returns a copy of this [NullaryPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [NullaryPhysicalOperatorNode].
     */
    abstract override fun copy(): NullaryPhysicalOperatorNode

    /**
     * Creates and returns a copy of this [NullaryLogicalOperatorNode].
     *
     * @return Copy of this [OperatorNode.Logical].
     */
    final override fun copyWithGroupInputs(): NullaryPhysicalOperatorNode = this.copy()

    /**
     * Creates and returns a copy of this [NullaryPhysicalOperatorNode].
     *
     * @return Copy of this [OperatorNode.Logical].
     */
    final override fun copyWithInputs(): NullaryPhysicalOperatorNode = this.copy()

    /**
     * Creates and returns a copy of this [NullaryPhysicalOperatorNode] with its output reaching down to the [root] of the tree.
     *
     * @param input The [OperatorNode.Physical]s that act as input. Must be empty!
     * @return Copy of this [NullaryPhysicalOperatorNode] with its output.
     */
    override fun copyWithOutput(vararg input: Physical): Physical {
        require(input.isEmpty()) { "Cannot provide input for NullaryPhysicalOperatorNode." }
        val copy = this.copy()
        return (this.output?.copyWithOutput(copy) ?: copy).root
    }

    /**
     * Performs late value binding using the given [QueryContext].
     *
     * By default, this operation has no effect. Override to implement operator specific binding.
     *
     * @param ctx [BindingContext] to use to resolve this [Binding].
     * @return This [OperatorNode].
     */
    override fun bindValues(ctx: BindingContext<Value>): OperatorNode = this

    /**
     * Calculates and returns the digest for this [NullaryPhysicalOperatorNode].
     *
     * @return Digest for this [NullaryPhysicalOperatorNode]
     */
    final override fun digest(): Long = this.hashCode().toLong()
}