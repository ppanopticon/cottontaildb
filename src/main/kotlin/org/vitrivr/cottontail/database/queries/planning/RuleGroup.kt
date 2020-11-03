package org.vitrivr.cottontail.database.queries.planning

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.RewriteRule

/**
 * A [RuleGroup] is a collection of [RewriteRule]s that can be applied to a [NodeExpression].
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
data class RuleGroup(val rules: Collection<RewriteRule>) {
    /**
     * Apples the rules contained in this [RuleGroup] to the given [NodeExpression]. If any rule
     * produces a result, it is added to the list of candidates
     *
     * @param expression The [NodeExpression] to apply rules to.
     * @return List of candidate [NodeExpression]s.
     */
    fun apply(expression: NodeExpression): List<NodeExpression> {
        val candidates = mutableListOf<NodeExpression>()
        for (rule in this.rules) {
            if (rule.canBeApplied(expression)) {
                val result = rule.apply(expression)
                if (result != null) {
                    candidates.add(result)
                }
            }
        }
        return candidates
    }
}