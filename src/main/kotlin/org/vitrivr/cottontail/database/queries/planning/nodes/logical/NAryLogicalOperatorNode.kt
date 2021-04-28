package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.Digest
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.BinaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.model.values.types.Value
import java.io.PrintStream
import java.util.*

/**
 * An abstract [OperatorNode.Logical] implementation that has multiple [OperatorNode.Logical]s as input.
 *
 * @author Ralph Gasser
 * @version 2.1.1
 */
abstract class NAryLogicalOperatorNode(vararg inputs: Logical) : OperatorNode.Logical() {

    /** The inputs to this [NAryLogicalOperatorNode]. The first input belongs to the same group. */
    private val _inputs: MutableList<Logical> = LinkedList<Logical>()
    val inputs: List<Logical>
        get() = Collections.unmodifiableList(this._inputs)

    /** A [BinaryLogicalOperatorNode]'s index is always the [depth] of its [left] input + 1. */
    final override var depth: Int = 0
        private set

    /**
     * The group ID of a [NAryLogicalOperatorNode] is always the one of its left parent.
     *
     * This is an (arbitrary) definition but very relevant when implementing [BinaryPhysicalOperatorNode]s.
     */
    final override val groupId: GroupId
        get() = this._inputs.firstOrNull()?.groupId ?: 0

    /** The [base] of a [NAryLogicalOperatorNode] is always itself. */
    final override val base: Collection<OperatorNode.Logical>
        get() = this._inputs.flatMap { it.base }

    /** By default, the [NAryLogicalOperatorNode] outputs the [ColumnDef] of its input. */
    override val columns: Array<ColumnDef<*>>
        get() = (this.inputs.firstOrNull()?.columns ?: emptyArray())

    /** By default, a [NAryLogicalOperatorNode]'s output is unordered. */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>> = emptyArray()

    /** By default, a [NAryLogicalOperatorNode] doesn't have any requirement. */
    override val requires: Array<ColumnDef<*>> = emptyArray()

    init {
        inputs.forEach { this.addInput(it) }
    }

    /**
     * Adds a [OperatorNode.Logical] to the list of inputs.
     *
     * @param input [OperatorNode.Logical] that should be added as input.
     */
    fun addInput(input: Logical) {
        require(input.output == null) { "Cannot connect $input to $this: Output is already occupied!" }
        if (this._inputs.size == 0) {
            this.depth = input.depth + 1
        }
        this._inputs.add(input)
        input.output = this
    }

    /**
     * Creates and returns a copy of this [NAryLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [NAryLogicalOperatorNode].
     */
    abstract override fun copy(): NAryLogicalOperatorNode

    /**
     * Creates and returns a copy of this [NAryLogicalOperatorNode] and all its inputs that belong to the same [GroupId],
     * up and until the base of the tree.
     *
     * @return Copy of this [NAryLogicalOperatorNode].
     */
    final override fun copyWithGroupInputs(): NAryLogicalOperatorNode {
        val copy = this.copy()
        val input = this.inputs.firstOrNull()?.copyWithGroupInputs()
        if (input != null) {
            copy.addInput(input)
        }
        return copy
    }

    /**
     * Creates and returns a copy of this [NAryLogicalOperatorNode] and all its inputs up and until the base of the tree.
     *
     * @return Copy of this [NAryLogicalOperatorNode].
     */
    final override fun copyWithInputs(): NAryLogicalOperatorNode {
        val copy = this.copy()
        this.inputs.forEach { copy.addInput(it.copyWithInputs()) }
        return copy
    }

    /**
     * Creates and returns a copy of this [NAryLogicalOperatorNode] with its output reaching down to the [root] of the tree.
     * Furthermore connects the provided [input] to the copied [OperatorNode.Logical]s.
     *
     * @param input The [OperatorNode.Logical]s that act as input.
     * @return Copy of this [NAryLogicalOperatorNode] with its output.
     */
    override fun copyWithOutput(vararg input: OperatorNode.Logical): Logical {
        val copy = this.copy()
        input.forEach { copy.addInput(it) }
        return (this.output?.copyWithOutput(copy) ?: copy).root
    }

    /**
     * Performs value binding using the given [BindingContext].
     *
     * [NAryLogicalOperatorNode] are required to propagate calls to [bindValues] up the tree in addition
     * to executing the binding locally. Consequently, the call is propagated to all input [OperatorNode]s.
     *
     * By default, this operation has no further effect. Override to implement operator specific binding but don't forget to call super.bindValues()
     *
     * @param ctx [BindingContext] to use to resolve [Binding]s.
     * @return This [NAryLogicalOperatorNode].
     */
    override fun bindValues(ctx: BindingContext<Value>): NAryLogicalOperatorNode {
        this._inputs.forEach { it.bindValues(ctx) }
        return this
    }

    /**
     * Calculates and returns the digest for this [NAryLogicalOperatorNode].
     *
     * @return [Digest] for this [NAryLogicalOperatorNode]
     */
    override fun digest(): Digest {
        var result = this.hashCode().toLong()
        for (i in this._inputs) {
            result = 33L * result + i.digest()
        }
        result += 33L * result + this.depth.hashCode()
        return result
    }

    /**
     * Prints this [OperatorNode] tree to the given [PrintStream].
     *
     * @param p The [PrintStream] to print this [OperatorNode] to. Defaults to [System.out]
     */
    override fun printTo(p: PrintStream) {
        this._inputs.forEach { it.printTo(p) }
        super.printTo(p)
    }
}