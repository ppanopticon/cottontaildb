package org.vitrivr.cottontail.database.queries.planning.nodes.physical

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.database.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.model.values.types.Value
import java.io.PrintStream

/**
 * An abstract [OperatorNode.Physical] implementation that has a single [OperatorNode] as input.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
abstract class UnaryPhysicalOperatorNode(input: Physical? = null) : OperatorNode.Physical() {

    /** The arity of the [UnaryPhysicalOperatorNode] is always on. */
    final override val inputArity = 1

    /** A [UnaryLogicalOperatorNode]'s index is always the [depth] of its [input] + 1. This is set in the [input]'s setter. */
    final override var depth: Int = 0
        private set

    /** The group Id of a [UnaryPhysicalOperatorNode] is always the one of its parent.*/
    final override val groupId: GroupId
        get() = this.input?.groupId ?: 0

    /** The input [OperatorNode.Logical]. */
    var input: Physical? = null
        protected set(value) {
            require(value?.output == null) { "Cannot connect $value to $this: Output is already occupied!" }
            field?.output = null
            value?.output = this
            this.depth = value?.depth?.plus(1) ?: 0
            field = value
        }

    /** The [base] of a [UnaryPhysicalOperatorNode] is always its parent's base. */
    final override val base: Collection<Physical>
        get() = this.input?.base ?: emptyList()

    /** The [totalCost] of a [UnaryPhysicalOperatorNode] is always the sum of its own and its input cost. */
    final override val totalCost: Cost
        get() = (this.input?.totalCost ?: Cost.ZERO) + this.cost

    /** By default, a [UnaryPhysicalOperatorNode] has no specific requirements. */
    override val requires: Array<ColumnDef<*>> = emptyArray()

    /** By default, [UnaryPhysicalOperatorNode]s are executable if their input is executable. */
    override val executable: Boolean
        get() = this.input?.executable ?: false

    /** By default, [UnaryPhysicalOperatorNode]s can be partitioned, if their parent can be partitioned. */
    override val canBePartitioned: Boolean
        get() = this.input?.canBePartitioned ?: false

    /** By default, the [UnaryPhysicalOperatorNode] outputs the [ColumnDef] of its input. */
    override val columns: Array<ColumnDef<*>>
        get() = (this.input?.columns ?: emptyArray())

    /** By default, the output size of a [UnaryPhysicalOperatorNode] is the same as its input's output size. */
    override val outputSize: Long
        get() = (this.input?.outputSize ?: 0)

    /** By default, a [UnaryPhysicalOperatorNode]'s order is the same as its input's order. */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>>
        get() = this.input?.order ?: emptyArray()

    /** By default, a [UnaryPhysicalOperatorNode]'s [RecordStatistics] is the same as its input's [RecordStatistics].*/
    override val statistics: RecordStatistics
        get() = this.input?.statistics ?: RecordStatistics.EMPTY

    init {
        this.input = input
    }

    /**
     * Creates and returns a copy of this [UnaryPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [UnaryPhysicalOperatorNode].
     */
    abstract override fun copy(): UnaryPhysicalOperatorNode

    /**
     * Creates and returns a copy of this [UnaryPhysicalOperatorNode] and all its inputs that belong to the same [GroupId],
     * up and until the base of the tree.
     *
     * @return Copy of this [OperatorNode.Logical].
     */
    final override fun copyWithGroupInputs(): UnaryPhysicalOperatorNode {
        val copy = this.copy()
        copy.input = this.input?.copyWithGroupInputs()
        return copy
    }

    /**
     * Creates and returns a copy of this [UnaryPhysicalOperatorNode] and all its inputs that belong to the same [GroupId],
     * up and until the base of the tree.
     *
     * @return Copy of this [OperatorNode.Physical].
     */
    final override fun copyWithInputs() = this.copyWithGroupInputs()

    /**
     * Creates and returns a copy of this [UnaryPhysicalOperatorNode] with its output reaching down to the [root] of the tree.
     * Furthermore connects the provided [input] to the copied [UnaryPhysicalOperatorNode]s.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [UnaryPhysicalOperatorNode] with its output.
     */
    override fun copyWithOutput(vararg input: Physical): Physical {
        require(input.size <= this.inputArity) { "Cannot provide more than ${this.inputArity} inputs for ${this.javaClass.simpleName}." }
        val copy = this.copy()
        copy.input = input.getOrNull(0)
        return (this.output?.copyWithOutput(copy) ?: copy).root
    }

    /**
     * Performs value binding using the given [BindingContext].
     *
     * [UnaryPhysicalOperatorNode] are required to propagate calls to [bindValues] up the tree in addition
     * to executing the binding locally. Consequently, the call is propagated to all the [input] [OperatorNode].
     *
     * By default, this operation has no further effect. Override to implement operator specific binding but don't forget to call super.bindValues()
     *
     * @param ctx [BindingContext] to use to resolve [Binding]s.
     * @return This [OperatorNode].
     */
    override fun bindValues(ctx: BindingContext<Value>): OperatorNode {
        this.input?.bindValues(ctx)
        return this
    }

    /**
     * Calculates and returns the digest for this [UnaryPhysicalOperatorNode].
     *
     * @return Digest for this [UnaryPhysicalOperatorNode]
     */
    final override fun digest(): Long {
        val result = 27L * this.hashCode() + (this.input?.digest() ?: -1L)
        return 27L * result + this.depth.hashCode()
    }

    /**
     * Prints this [OperatorNode] tree to the given [PrintStream].
     *
     * @param p The [PrintStream] to print this [OperatorNode] to. Defaults to [System.out]
     */
    override fun printTo(p: PrintStream) {
        this.input?.printTo(p)
        super.printTo(p)
    }
}

