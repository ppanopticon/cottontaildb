package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.model.values.types.Value
import java.io.PrintStream

/**
 * An abstract [OperatorNode.Logical] implementation that has exactly two [OperatorNode.Logical]s as input.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
abstract class BinaryLogicalOperatorNode(left: Logical? = null, right: Logical? = null) : OperatorNode.Logical() {

    /** Input arity of [UnaryLogicalOperatorNode] is always two. */
    final override val inputArity: Int = 2

    /** A [BinaryLogicalOperatorNode]'s index is always the [depth] of its [left] input + 1. */
    final override var depth: Int = 0
        private set

    /** The left branch of the input; belongs to the same group! */
    var left: Logical? = null
        set(value) {
            require(value?.output == null) { "Cannot connect $value to $this: Output is already occupied!" }
            field?.output = null
            value?.output = this
            this.depth = value?.depth?.plus(1) ?: 0
            field = value
        }

    /** The right branch of the input; constitutes a new group! */
    var right: Logical? = null
        set(value) {
            require(value?.output == null) { "Cannot connect $value to $this: Output is already occupied!" }
            field?.output = null
            value?.output = this
            field = value
        }

    /**
     * The group ID of a [BinaryLogicalOperatorNode] is always the one of its left parent.
     *
     * This is an (arbitrary) definition but very relevant when implementing [BinaryLogicalOperatorNode]s.
     */
    final override val groupId: GroupId
        get() = this.left?.groupId ?: 0

    /** The [base] of a [BinaryLogicalOperatorNode] is the sum of its input's bases. */
    final override val base: Collection<Logical>
        get() = (this.left?.base ?: emptyList()) + (this.right?.base ?: emptyList())

    /** By default, a [BinaryLogicalOperatorNode]'s order is unspecified. */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>> = emptyArray()

    /** By default, a [BinaryLogicalOperatorNode]'s requirements are empty. */
    override val requires: Array<ColumnDef<*>> = emptyArray()

    init {
        this.left = left
        this.right = right
    }

    /**
     * Creates and returns a copy of this [BinaryLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [BinaryLogicalOperatorNode].
     */
    abstract override fun copy(): BinaryLogicalOperatorNode

    /**
     * Creates and returns a copy of this [BinaryLogicalOperatorNode] and all its inputs that belong to the same [GroupId],
     * up and until the base of the tree.
     *
     * @return Copy of this [OperatorNode.Logical].
     */
    final override fun copyWithGroupInputs(): BinaryLogicalOperatorNode {
        val copy = this.copy()
        copy.left = this.left?.copyWithGroupInputs()
        return copy
    }

    /**
     * Creates and returns a copy of this [BinaryLogicalOperatorNode] and all its inputs up and until the base of the tree.
     *
     * @return Copy of this [OperatorNode.Logical].
     */
    final override fun copyWithInputs(): BinaryLogicalOperatorNode {
        val copy = this.copyWithGroupInputs()
        copy.right = this.right?.copyWithInputs()
        return copy
    }

    /**
     * Creates and returns a copy of this [BinaryLogicalOperatorNode] with its output reaching down to the [root] of the tree.
     * Furthermore connects the provided [input] to the copied [OperatorNode.Logical]s.
     *
     * @param input The [OperatorNode.Logical]s that act as input.
     * @return Copy of this [OperatorNode.Logical] with its output.
     */
    override fun copyWithOutput(vararg input: Logical): Logical {
        require(input.size <= this.inputArity) { "Cannot provide more than ${this.inputArity} inputs for ${this.javaClass.simpleName}." }
        val copy = this.copy()
        copy.left = input.getOrNull(0)
        copy.right = input.getOrNull(1)
        return (this.output?.copyWithOutput(copy) ?: copy).root
    }

    /**
     * Performs late value binding using the given [BindingContext].
     *
     * [OperatorNode] are required to propagate calls to [bindValues] up the tree in addition
     * to executing the binding locally. Consequently, the call is propagated to all input [OperatorNode]
     * of this [OperatorNode].
     *
     * @param ctx [BindingContext] to use to resolve this [Binding].
     * @return This [OperatorNode].
     */
    override fun bindValues(ctx: BindingContext<Value>): OperatorNode {
        this.left?.bindValues(ctx)
        this.right?.bindValues(ctx)
        return this
    }

    /**
     * Calculates and returns the digest for this [BinaryLogicalOperatorNode].
     *
     * @return Digest for this [BinaryLogicalOperatorNode]
     */
    override fun digest(): Long {
        var result = 33L * hashCode() + (this.left?.digest() ?: -1L)
        result = 33L * result + (this.right?.digest() ?: -1L)
        return 33L * result + this.depth.hashCode()
    }

    /**
     * Prints this [OperatorNode] tree to the given [PrintStream].
     *
     * @param p The [PrintStream] to print this [OperatorNode] to. Defaults to [System.out]
     */
    override fun printTo(p: PrintStream) {
        this.left?.printTo(p)
        this.right?.printTo(p)
        super.printTo(p)
    }
}