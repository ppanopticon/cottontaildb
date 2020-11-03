package org.vitrivr.cottontail.database.queries.planning

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.PhysicalNodeExpression
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * This is a rather simple query planner that optimizes a [NodeExpression] by recursively applying
 * a set of [RewriteRule]s to get more sophisticated yet equivalent [NodeExpression]s.
 *
 * Query optimization takes place in two stages:
 *
 * During the first stage, the logical tree is rewritten by means of other [LogicalNodeExpression]s,
 * to generate several, equivalent representations of the query.
 *
 * During the second stage, the logical tree is rewritten by replacing [LogicalNodeExpression]s by
 * [PhysicalNodeExpression] to arrive at an executable query plan. Optimization during the second
 * stage is done based on estimated costs.
 *
 * @author Ralph Gasser
 * @version 1.1.1
 */
class CottontailQueryPlanner(logicalRewriteRules: Collection<RewriteRule>, physicalRewriteRules: Collection<RewriteRule>) {

    /** The [RuleGroup] for the logical rewrite phase. */
    private val logicalShuttle = RuleGroup(logicalRewriteRules)

    /** The [RuleGroup] for the physical rewrite phase. */
    private val physicalShuttle = RuleGroup(physicalRewriteRules)

    /**
     * Generates a list of equivalent [NodeExpression]s by recursively applying [RewriteRule]s
     * on the seed [NodeExpression] and derived [NodeExpression]. The level of recursion and the number
     * of candidates to consider per level can be configured.
     *
     * @param expression The [NodeExpression] to optimize.
     * @param recursion The depth of recursion before final candidate is selected.
     * @param candidatesPerLevel The number of candidates to generate per recursion level.
     *
     * @throws QueryException.QueryPlannerException If planner fails to generate a valid execution plan.
     */
    fun plan(expression: LogicalNodeExpression): Collection<PhysicalNodeExpression> {
        /** Generate stage 1 candidates by logical optimization. */
        val stage1 = (this.optimize(expression, this.logicalShuttle) + expression)

        /** Generate stage 2 candidates by physical optimization. */
        val stage2 = stage1.flatMap {
            this.optimize(it, this.physicalShuttle)
        }.filter {
            it.root.executable
        }.filterIsInstance<PhysicalNodeExpression>()
        if (stage2.isEmpty()) {
            throw QueryException.QueryPlannerException("Failed to generate a physical execution plan for expression: $expression.")
        } else {
            return stage2
        }
    }

    /**
     * Performs optimization of a [LogicalNodeExpression] tree, by applying plan rewrite rules that
     * manipulate that tree and return equivalent [LogicalNodeExpression] trees.
     *
     * @param expression The [LogicalNodeExpression] that should be optimized.
     */
    fun optimize(expression: NodeExpression, group: RuleGroup): Collection<NodeExpression> {
        val candidates = mutableListOf<NodeExpression>()
        val explore = mutableListOf<NodeExpression>()
        var pointer: NodeExpression? = expression
        while (pointer != null) {

            /* Apply rules to node and add results to list for exploration. */
            val results = group.apply(pointer)
            if (results.isEmpty()) {
                if (pointer.inputs.size > 0) {
                    explore.addAll(pointer.inputs)
                } else {
                    candidates.add(pointer.root)
                }
            }
            for (r in results) {
                if (r.inputs.size > 0) {
                    explore.addAll(r.inputs)
                } else {
                    candidates.add(r.root)
                }
            }

            /* Move pointer up in the tree. */
            pointer = explore.removeLastOrNull()
        }
        return candidates
    }
}