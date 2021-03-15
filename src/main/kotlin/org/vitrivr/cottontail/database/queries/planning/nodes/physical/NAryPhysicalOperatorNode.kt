package org.vitrivr.cottontail.database.queries.planning.nodes.physical

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.NAryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.database.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.model.values.types.Value
import java.io.PrintStream
import java.util.*

/**
 * An abstract [OperatorNode.Physical] implementation that has multiple [OperatorNode.Physical]s as input.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
abstract class NAryPhysicalOperatorNode(vararg inputs: Physical) : OperatorNode.Physical() {

    /** The inputs to this [NAryLogicalOperatorNode]. The first input belongs to the same group. */
    private val _inputs: MutableList<Physical> = LinkedList<Physical>()
    val inputs: List<Physical>
        get() = Collections.unmodifiableList(this._inputs)

    /** A [NAryPhysicalOperatorNode]'s index is always the [depth] of its first input + 1. */
    final override var depth: Int = 0
        private set

    /**
     * The group ID of a [NAryPhysicalOperatorNode] is always the one of its left parent.
     *
     * This is an (arbitrary) definition but very relevant when implementing [BinaryPhysicalOperatorNode]s.
     */
    final override val groupId: GroupId
        get() = this.inputs.firstOrNull()?.groupId ?: 0

    /** The [base] of a [NAryPhysicalOperatorNode] is the sum of its input's bases. */
    final override val base: Collection<Physical>
        get() = this.inputs.flatMap { it.base }

    /** The [totalCost] of a [NAryPhysicalOperatorNode] is the sum of its own and its input cost. */
    final override val totalCost: Cost
        get() {
            var cost = this.cost
            for (i in inputs) {
                cost += i.totalCost
            }
            return cost
        }

    /** By default, a [NAryPhysicalOperatorNode]'s order is unspecified. */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>> = emptyArray()

    /** By default, a [NAryPhysicalOperatorNode]'s requirements are empty. */
    override val requires: Array<ColumnDef<*>> = emptyArray()

    /** [NAryPhysicalOperatorNode]s are executable if all their inputs are executable. */
    override val executable: Boolean
        get() = (this.inputs.size == this.inputArity) && this.inputs.all { it.executable }

    /** [NAryPhysicalOperatorNode] usually cannot be partitioned. */
    override val canBePartitioned: Boolean = false

    /** By default, the [NAryPhysicalOperatorNode] outputs the [ColumnDef] of its input. */
    override val columns: Array<ColumnDef<*>>
        get() = (this.inputs.firstOrNull()?.columns ?: emptyArray())

    /** By default, a [UnaryPhysicalOperatorNode]'s [RecordStatistics] is retained. */
    override val statistics: RecordStatistics
        get() = this.inputs.firstOrNull()?.statistics ?: RecordStatistics.EMPTY

    init {
        inputs.forEach { this.addInput(it) }
    }

    /**
     * Adds a [OperatorNode.Physical] to the list of inputs.
     *
     * @param input [OperatorNode.Physical] that should be added as input.
     */
    fun addInput(input: Physical) {
        require(input.output == null) { "Cannot connect $input to $this: Output is already occupied!" }
        if (this._inputs.size == 0) {
            this.depth = input.depth + 1
        }
        this._inputs.add(input)
        input.output = this
    }

    /**
     * Creates and returns a copy of this [NAryPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [NAryPhysicalOperatorNode].
     */
    abstract override fun copy(): NAryPhysicalOperatorNode

    /**
     * Creates and returns a copy of this [NAryPhysicalOperatorNode] and all its inputs that belong to the same [GroupId],
     * up and until the base of the tree.
     *
     * @return Copy of this [NAryPhysicalOperatorNode].
     */
    final override fun copyWithGroupInputs(): NAryPhysicalOperatorNode {
        val copy = this.copy()
        val input = this.inputs.firstOrNull()?.copyWithGroupInputs()
        if (input != null) {
            copy.addInput(input)
        }
        return copy
    }

    /**
     * Creates and returns a copy of this [NAryPhysicalOperatorNode] and all its inputs up and until the base of the tree.
     *
     * @return Copy of this [NAryPhysicalOperatorNode].
     */
    final override fun copyWithInputs(): NAryPhysicalOperatorNode {
        val copy = this.copy()
        this.inputs.forEach { copy.addInput(it.copyWithInputs()) }
        return copy
    }

    /**
     * Creates and returns a copy of this [NAryPhysicalOperatorNode] with its output reaching down to the [root] of the tree.
     * Furthermore connects the provided [input] to the copied [OperatorNode.Physical]s.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [NAryPhysicalOperatorNode] with its output.
     */
    override fun copyWithOutput(vararg input: Physical): Physical {
        val copy = this.copy()
        input.forEach { copy.addInput(it) }
        return (this.output?.copyWithOutput(copy) ?: copy).root
    }

    /**
     * Performs late value binding using the given [BindingContext].
     *
     * [NAryPhysicalOperatorNode] are required to propagate calls to [bindValues] up the tree in addition
     * to executing the binding locally. Consequently, the call is propagated to all input [OperatorNode]s.
     *
     * By default, this operation has no further effect. Override to implement operator specific binding but don't forget to call super.bindValues()
     *
     * @param ctx [BindingContext] to use to resolve [Binding]s.
     * @return This [OperatorNode].
     */
    override fun bindValues(ctx: BindingContext<Value>): OperatorNode {
        this.inputs.forEach { it.bindValues(ctx) }
        return this
    }

    /**
     * Calculates and returns the digest for this [BinaryPhysicalOperatorNode].
     *
     * @return Digest for this [BinaryPhysicalOperatorNode]
     */
    override fun digest(): Long {
        var result = this.hashCode().toLong()
        for (i in this.inputs) {
            result += 27L * result + i.digest()
        }
        result += 27L * result + this.depth.hashCode()
        return result
    }

    /**
     * Prints this [OperatorNode] tree to the given [PrintStream].
     *
     * @param p The [PrintStream] to print this [OperatorNode] to. Defaults to [System.out]
     */
    override fun printTo(p: PrintStream) {
        this.inputs.forEach { it.printTo(p) }
        super.printTo(p)
    }
}