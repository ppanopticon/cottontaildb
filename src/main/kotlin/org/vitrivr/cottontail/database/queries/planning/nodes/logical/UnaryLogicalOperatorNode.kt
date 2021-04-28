package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.Digest
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.model.values.types.Value
import java.io.PrintStream

/**
 * An abstract [OperatorNode.Logical] implementation that has a single [OperatorNode] as input.
 *
 * @author Ralph Gasser
 * @version 2.1.1
 */
abstract class UnaryLogicalOperatorNode(input: Logical? = null) : OperatorNode.Logical() {
    /** Input arity of [UnaryLogicalOperatorNode] is always one. */
    final override val inputArity: Int = 1

    /** A [UnaryLogicalOperatorNode]'s index is always the [depth] of its [input] + 1. */
    final override var depth: Int = 0
        private set

    /** The group Id of a [UnaryLogicalOperatorNode] is always the one of its parent.*/
    final override val groupId: GroupId
        get() = this.input?.groupId ?: 0

    /** The [base] of a [UnaryLogicalOperatorNode] is always its [input]'s base. */
    final override val base: Collection<Logical>
        get() = this.input?.base ?: emptyList()

    /** The input [OperatorNode.Logical]. */
    var input: Logical? = null
        set(value) {
            require(value?.output == null) { "Cannot connect $value to $this: Output is already occupied!" }
            field?.output = null
            value?.output = this
            this.depth = value?.depth?.plus(1) ?: 0
            field = value
        }

    /** By default, a [UnaryLogicalOperatorNode]'s order is retained. */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>>
        get() = this.input?.order ?: emptyArray()

    /** By default, a [UnaryLogicalOperatorNode]'s requirements are unspecified. */
    override val requires: Array<ColumnDef<*>> = emptyArray()

    init {
        this.input = input
    }

    /**
     * Creates and returns a copy of this [UnaryLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [UnaryLogicalOperatorNode].
     */
    abstract override fun copy(): UnaryLogicalOperatorNode

    /**
     * Creates and returns a copy of this [UnaryLogicalOperatorNode] and all its inputs that belong to the same [GroupId],
     * up and until the base of the tree.
     *
     * @return Copy of this [OperatorNode.Logical].
     */
    final override fun copyWithGroupInputs(): Logical {
        val copy = this.copy()
        copy.input = this.input?.copyWithGroupInputs()
        return copy
    }

    /**
     * Creates and returns a copy of this [UnaryLogicalOperatorNode] and all its inputs that belong to the same [GroupId],
     * up and until the base of the tree.
     *
     * @return Copy of this [OperatorNode.Logical].
     */
    final override fun copyWithInputs(): Logical = this.copyWithGroupInputs()

    /**
     * Creates and returns a copy of this [UnaryLogicalOperatorNode] with its output reaching down to the [root] of the tree.
     * Furthermore connects the provided [input] to the copied [UnaryLogicalOperatorNode]s.
     *
     * @param input The [OperatorNode.Logical]s that act as input.
     * @return Copy of this [UnaryLogicalOperatorNode] with its output.
     */
    override fun copyWithOutput(vararg input: Logical): Logical {
        require(input.size <= this.inputArity) { "Cannot provide more than ${this.inputArity} inputs for ${this.javaClass.simpleName}." }
        val copy = this.copy()
        copy.input = input.getOrNull(0)
        return (this.output?.copyWithOutput(copy) ?: copy).root
    }

    /**
     * Performs value binding using the given [QueryContext].
     *
     * [UnaryLogicalOperatorNode] are required to propagate calls to [bindValues] up the tree in addition
     * to executing the binding locally. Consequently, the call is propagated to all the [input] [OperatorNode].
     *
     * By default, this operation has no further effect. Override to implement operator specific binding but don't forget to call super.bindValues()
     *
     * @param ctx [BindingContext] to use to resolve this [Binding].
     * @return This [OperatorNode].
     */
    override fun bindValues(ctx: BindingContext<Value>): OperatorNode {
        this.input?.bindValues(ctx)
        return this
    }

    /**
     * Calculates and returns the digest for this [UnaryLogicalOperatorNode].
     *
     * @return [Digest] for this [UnaryLogicalOperatorNode]
     */
    final override fun digest(): Digest {
        val result = 33L * this.hashCode() + (this.input?.digest() ?: -1L)
        return 33L * result + this.depth.hashCode()
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