package org.vitrivr.cottontail.database.queries

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.planning.RuleGroup

/**
 * [OperatorNode]s are [Node]s in a Cottontail DB query execution plan and represent flow of
 * information. [OperatorNode]s take [org.vitrivr.cottontail.model.basics.Record]s as input
 * and transform them into [org.vitrivr.cottontail.model.basics.Record] output. The relationship
 * of input to output can be m to n.
 *
 * [OperatorNode]s allow for reasoning and transformation of the execution plan during query
 * optimization and are manipulated by the query planner.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
abstract class OperatorNode : Node {

    /** The arity of this [OperatorNode], i.e., the number of parents or inputs allowed. */
    abstract val inputArity: Int

    /** Whether or not this [OperatorNode] is executable. */
    abstract val executable: Boolean

    /**
     * Returns the root of this [OperatorNode], i.e., the final [OperatorNode] in terms of operation
     * that usually produces the output.
     */
    val root: OperatorNode
        get() = this.output?.root ?: this

    /**
     * The [OperatorNode] that receives the results produced by this [OperatorNode] as input.
     *
     * May be null, which makes this [OperatorNode] the root of the [OperatorNode] tree.
     */
    var output: OperatorNode? = null
        protected set

    /** The [OperatorNode] provides the inputs for this [OperatorNode]. */
    val inputs: MutableList<OperatorNode> = mutableListOf()

    /** The [ColumnDef]s produced by this [OperatorNode]. */
    abstract val columns: Array<ColumnDef<*>>

    /** The [RuleGroup] that last visited this [OperatorNode] */
    var lastVisitor: RuleGroup? = null

    /**
     * Updates the [OperatorNode.output] of this [OperatorNode] to the given [OperatorNode].
     *
     * This method should take care of updating the child's parents as well as the old child's parents
     * (if such exist).
     *
     * @param input The [OperatorNode] that should act as new input of this [OperatorNode].
     * @return Reference to the input [OperatorNode]
     */
    fun <T : OperatorNode> addInput(input: T): T {
        check(this.inputArity > this.inputs.size) { "Cannot add input NodeExpression because $this already has enough inputs (arity = ${this.inputArity})." }
        this.inputs.add(input)
        input.output = this
        return input
    }

    /**
     * Performs late value binding using the given [QueryContext].
     *
     * [OperatorNode] are required to propagate calls to [bindValues] up the tree in addition
     * to executing the binding locally. Consequently, the call is propagated to all input [OperatorNode]
     * of this [OperatorNode].
     *
     * @param ctx [QueryContext] to use to resolve this [Binding].
     * @return This [OperatorNode].
     */
    override fun bindValues(ctx: QueryContext): OperatorNode {
        for (i in this.inputs) {
            i.bindValues(ctx)
        }
        return this
    }

    /**
     * Calculates and returns the deep digest for this [OperatorNode]. Consists of a combination
     * of all digests up from the start of the tree up and until this [OperatorNode],
     *
     * Consequently, the call is propagated to all input [OperatorNode] of this [OperatorNode].
     *
     * @return Deep digest for this [OperatorNode]
     */
    fun deepDigest(): Long {
        var digest = this.digest()
        for (i in this.inputs) {
            digest = 31 * digest + i.deepDigest()
        }
        return digest
    }

    /**
     * Creates and returns an exact copy of this [OperatorNode] without any [inputs] or [output]s.
     *
     * @return Copy of this [OperatorNode].
     */
    abstract fun copy(): OperatorNode

    /**
     * Creates and returns a deep copy of this [OperatorNode] including a deep copy of all incoming
     * [OperatorNode]s
     *
     * @return Exact copy of this [OperatorNode] and all parent [OperatorNode]s,
     */
    fun deepCopy(): OperatorNode {
        val t = this.copy()
        for (p in this.inputs) {
            val c = p.deepCopy()
            t.addInput(c)
        }
        return t
    }

    /**
     * Creates and returns a copy of all outgoing [OperatorNode] to this [OperatorNode].
     * May be null, if this [OperatorNode] has no children.
     *
     * @return Exact copy of this [OperatorNode]'s child and its children.
     */
    fun copyOutput(): OperatorNode? {
        val c = this.output
        return if (c != null) {
            val cc = c.copy()
            c.copyOutput()?.addInput(cc)
            cc
        } else {
            null
        }
    }
}