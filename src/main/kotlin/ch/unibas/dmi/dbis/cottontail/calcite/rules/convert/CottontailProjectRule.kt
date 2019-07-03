package ch.unibas.dmi.dbis.cottontail.calcite.rules.convert

import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailProject

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.plan.RelOptRuleCall


/**
 * A rule to convert a [LogicalProject] to a [CottontailProject].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object CottontailProjectRule : CottontailConverterRule<LogicalProject>(LogicalProject::class.java, "CtProject") {

    /**
     * This rule only matches, of the projected fields are all actual columns found in the [Entity]
     *
     * @param call The [RelOptRuleCall] to check.
     */
    override fun matches(call: RelOptRuleCall): Boolean {
        val project = call.rel<LogicalProject>(0)
        for (e in project.projects) {
            if (e !is RexInputRef) {
                return false
            }
        }
        return true
    }

    /**
     * Converts the given [RelNode] to a [CottontailProject].
     *
     * @param rel The [RelNode] to convert.
     */
    override fun convert(rel: RelNode): RelNode {
        val project = rel as LogicalProject
        val traitSet = project.traitSet.replace(out)
        return CottontailProject(project.cluster, traitSet, RelOptRule.convert(project.input, out), project.projects, project.rowType)
    }
}
