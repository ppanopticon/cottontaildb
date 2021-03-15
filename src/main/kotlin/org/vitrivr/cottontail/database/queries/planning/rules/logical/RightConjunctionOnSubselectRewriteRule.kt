package org.vitrivr.cottontail.database.queries.planning.rules.logical

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterOnSubSelectLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ConnectionOperator

/**
 * Decomposes a [FilterOnSubSelectLogicalOperatorNode] that contains a [BooleanPredicate.Compound]
 * connected with a [ConnectionOperator.AND] into a sequence of two [FilterOnSubSelectLogicalOperatorNode]s or
 * a [FilterOnSubSelectLogicalOperatorNode] and a [FilterLogicalOperatorNode].
 *
 * Gives precedence to the right operand.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object RightConjunctionOnSubselectRewriteRule : RewriteRule {

    /**
     * Checks if this [LeftConjunctionOnSubselectRewriteRule] can be applied to the given [OperatorNode].
     *
     * @param node The input [OperatorNode] to check.
     * @return True if [RewriteRule] can be applied, false otherwise.
     */
    override fun canBeApplied(node: OperatorNode): Boolean =
        node is FilterOnSubSelectLogicalOperatorNode &&
                node.predicate is BooleanPredicate.Compound &&
                node.predicate.connector == ConnectionOperator.AND


    /**
     * Decomposes the provided [FilterOnSubSelectLogicalOperatorNode] with a conjunction into two consecutive
     * [FilterLogicalOperatorNode]s, where each resulting [FilterLogicalOperatorNode] covers
     * one part of the conjunction. Gives precedence to the left part of the conjunction.
     *
     * @param node The input [OperatorNode].
     * @param ctx The [QueryContext] in which query planning takes place.
     *
     * @return The output [OperatorNode] or null, if no rewrite was done.
     */
    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is FilterOnSubSelectLogicalOperatorNode && node.predicate is BooleanPredicate.Compound && node.predicate.connector == ConnectionOperator.AND) {
            val parent = node.inputs[0].copyWithInputs()
            val p1DependsOn = node.predicate.p1.atomics.filterIsInstance<BooleanPredicate.Atomic.Literal>().filter { it.dependsOn > -1 }
            val p2DependsOn = node.predicate.p2.atomics.filterIsInstance<BooleanPredicate.Atomic.Literal>().filter { it.dependsOn > -1 }

            val p2Filter = if (p2DependsOn.isNotEmpty()) {
                FilterOnSubSelectLogicalOperatorNode(node.predicate.p2, parent)
            } else {
                FilterLogicalOperatorNode(parent, node.predicate.p2)
            }

            val p1Filter = if (p1DependsOn.isNotEmpty()) {
                FilterOnSubSelectLogicalOperatorNode(node.predicate.p1, p2Filter)
            } else {
                FilterLogicalOperatorNode(p2Filter, node.predicate.p1)
            }

            return node.output?.copyWithOutput(p1Filter) ?: p1Filter
        }
        return null
    }
}