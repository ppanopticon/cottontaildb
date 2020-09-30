package org.vitrivr.cottontail.database.queries.planning

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
data class RuleShuttle(val rules: Collection<RewriteRule>) {
    /**
     * Apples the rules contained in this [RuleShuttle] to the given [NodeExpression]. If any rule
     * produces a result, it is added to the list of candidates
     *
     * @param expression The [NodeExpression] to apply rules to.
     * @param candidates The candidates to add transformed [NodeExpression]s to.
     */
    fun apply(expression: NodeExpression, candidates: MutableList<NodeExpression>) {
        for (rule in this.rules) {
            if (rule.canBeApplied(expression)) {
                val result = rule.apply(expression)
                if (result != null) {
                    candidates.add(result.root)
                }
            }
        }
    }
}