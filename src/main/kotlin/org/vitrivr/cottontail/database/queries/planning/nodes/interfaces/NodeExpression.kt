package org.vitrivr.cottontail.database.queries.planning.nodes.interfaces

import org.vitrivr.cottontail.database.queries.planning.RuleShuttle
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalNodeExpression

/**
 * [NodeExpression]s are components in the Cottontail DB execution plan and represent flow of
 * information. [NodeExpression]s take 0 to n [org.vitrivr.cottontail.model.recordset.Recordset]s
 * as input and transform them into a single output [org.vitrivr.cottontail.model.recordset.Recordset].
 *
 * [NodeExpression]s allow for reasoning and transformation of the execution plan during query optimization.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
abstract class NodeExpression {

    companion object {

        /**
         * Tries to seek the base of the provided [NodeExpression], i.e. its start or source, and returns it.
         * Bases can only be determined, if a [NodeExpression] doesn't branch.
         *
         * @param node [NodeExpression] The [NodeExpression] to seek the base for.
         * @return Base / Source [NodeExpression] or null, if [NodeExpression] tree contains branches.
         */
        fun seekBase(node: NodeExpression): NodeExpression? = when (node) {
            is UnaryPhysicalNodeExpression -> seekBase(node.input)
            is NullaryPhysicalNodeExpression ->  node
            else -> null
        }
    }


    /** The arity of this [NodeExpression], i.e., the number of parents or inputs allowed. */
    abstract val inputArity: Int

    /** Whether or not this [NodeExpression] is executable. */
    abstract val executable: Boolean

    /** Returns the base of this [NodeExpression], i.e., start of the [NodeExpression] tree. */
    val base: NodeExpression
        get() = this.inputs.firstOrNull() ?: this

    /** Returns the root of this [NodeExpression], i.e., end of the [NodeExpression] tree. */
    val root: NodeExpression
        get() = this.output?.root ?: this

    /**
     * The [NodeExpression] that receives the results produced by this [NodeExpression] as input.
     *
     * May be null, which makes this [NodeExpression] the root of the [NodeExpression] tree.
     */
    var output: NodeExpression? = null
        protected set

    /** The [NodeExpression] provides the inputs for this [NodeExpression]. */
    val inputs: MutableList<NodeExpression> = mutableListOf()

    /** The [RuleShuttle] that last visited this [NodeExpression] */
    var lastVisitor: RuleShuttle? = null

    /**
     * Applies the [RewriteRule]s contained in the given [RuleShuttle] to this [NodeExpression]
     * and, if a transformation can take place, adds the resulting [NodeExpression] to the list
     * of candidates. Then passes the [RuleShuttle] up the tree.
     *
     * @param shuttle The [RuleShuttle] to apply.
     * @param candidates The list of candidates.
     */
    fun apply(shuttle: RuleShuttle, candidates: MutableList<NodeExpression>) {
        if (this.lastVisitor != shuttle) {
            shuttle.apply(this, candidates)
            this.inputs.forEach { it.apply(shuttle, candidates) }
            this.lastVisitor = shuttle
        }
    }

    /**
     * Updates the [NodeExpression.output] of this [NodeExpression] to the given [NodeExpression].
     *
     * This method should take care of updating the child's parents as well as the old child's parents
     * (if such exist).
     *
     * @param input The [NodeExpression] that should act as new input of this [NodeExpression].
     * @return Reference to the input [NodeExpression]
     */
    fun <T: NodeExpression> addInput(input: T): T {
        check(this.inputArity > this.inputs.size) { "Cannot add input NodeExpression because $this already has enough inputs (arity = ${this.inputArity})."}
        this.inputs.add(input)
        input.output = this
        return input
    }

    /**
     * Creates and returns a copy of this [NodeExpression] without any [inputs] or [output]s.
     *
     * @return Copy of this [NodeExpression].
     */
    abstract fun copy(): NodeExpression

    /**
     * Creates and returns a copy of the tree up and until this [NodeExpression]. Includes at least
     * one [NodeExpression] which is the current [NodeExpression]
     *
     * @return Exact copy of this [NodeExpression] and all parent [NodeExpression]s,
     */
    fun copyWithInputs(): NodeExpression {
        val t = this.copy()
        for (p in this.inputs) {
            val c = p.copyWithInputs()
            t.addInput(c)
        }
        return t
    }

    /**
     * Creates and returns a copy of the tree down from this [NodeExpression]. May be null, if this
     * [NodeExpression] has no children.
     *
     * @return Exact copy of this [NodeExpression]'s child and its children.
     */
    fun copyOutput(): NodeExpression? {
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