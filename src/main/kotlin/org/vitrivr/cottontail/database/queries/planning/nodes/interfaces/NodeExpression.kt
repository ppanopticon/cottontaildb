package org.vitrivr.cottontail.database.queries.planning.nodes.interfaces

import org.vitrivr.cottontail.database.queries.planning.RuleGroup
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * [NodeExpression]s are components in the Cottontail DB execution plan and represent flow of
 * information. [NodeExpression]s take 0 to n [org.vitrivr.cottontail.model.basics.Record]s
 * as input and transform them into a 0 to m [org.vitrivr.cottontail.model.basics.Record] output.
 *
 * [NodeExpression]s allow for reasoning and transformation of the execution plan during query
 * optimization.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
abstract class NodeExpression {

    /** The arity of this [NodeExpression], i.e., the number of parents or inputs allowed. */
    abstract val inputArity: Int

    /** Whether or not this [NodeExpression] is executable. */
    abstract val executable: Boolean

    /** Returns the base of this [NodeExpression], i.e., start of the [NodeExpression] tree. */
    val base: NodeExpression
        get() = this.inputs.firstOrNull()?.base ?: this

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

    /** The [ColumnDef]s produced by this [NodeExpression]. */
    abstract val columns: Array<ColumnDef<*>>

    /** The [RuleGroup] that last visited this [NodeExpression] */
    var lastVisitor: RuleGroup? = null

    /**
     * Updates the [NodeExpression.output] of this [NodeExpression] to the given [NodeExpression].
     *
     * This method should take care of updating the child's parents as well as the old child's parents
     * (if such exist).
     *
     * @param input The [NodeExpression] that should act as new input of this [NodeExpression].
     * @return Reference to the input [NodeExpression]
     */
    fun <T : NodeExpression> addInput(input: T): T {
        check(this.inputArity > this.inputs.size) { "Cannot add input NodeExpression because $this already has enough inputs (arity = ${this.inputArity})." }
        this.inputs.add(input)
        input.output = this
        return input
    }

    /**
     * Calculates and returns the digest for this [NodeExpression], which should uniquely identify
     * this [NodeExpression], similarly to [hashCode].
     *
     * @return Digest for this [NodeExpression]
     */
    abstract fun digest(): Long

    /**
     * Calculates and returns the deep digest for this [NodeExpression]. Consists of a combination
     * of all digests up from the base of the tree up and until this [NodeExpression].
     *
     * @return Deep digest for this [NodeExpression]
     */
    fun deepDigest(): Long {
        var digest = this.digest()
        for (i in this.inputs) {
            digest = 31 * digest + i.deepDigest()
        }
        return digest
    }

    /**
     * Creates and returns an exact copy of this [NodeExpression] without any [inputs] or [output]s.
     *
     * @return Copy of this [NodeExpression].
     */
    abstract fun copy(): NodeExpression

    /**
     * Creates and returns a deep copy of this [NodeExpression] including a deep copy of all incoming
     * [NodeExpression]s
     *
     * @return Exact copy of this [NodeExpression] and all parent [NodeExpression]s,
     */
    fun deepCopy(): NodeExpression {
        val t = this.copy()
        for (p in this.inputs) {
            val c = p.deepCopy()
            t.addInput(c)
        }
        return t
    }

    /**
     * Creates and returns a copy of all outgoing [NodeExpression] to this [NodeExpression].
     * May be null, if this [NodeExpression] has no children.
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