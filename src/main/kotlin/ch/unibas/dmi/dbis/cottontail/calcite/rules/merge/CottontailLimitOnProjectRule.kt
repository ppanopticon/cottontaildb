package ch.unibas.dmi.dbis.cottontail.calcite.rules.merge

import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailLimit
import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailProject
import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailRel
import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailTableScan
import ch.unibas.dmi.dbis.cottontail.calcite.rules.CottontailToEnumerableConverter

import org.apache.calcite.adapter.enumerable.EnumerableLimit

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rex.RexInputRef

/**
 * A rule to combine a sequence of [CottontailProject] and [CottontailLimitOnScanRule] into a single call of [CottontailTableScan]
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object CottontailLimitOnProjectRule : RelOptRule(operand(CottontailLimit::class.java, operand(CottontailProject::class.java, operand(CottontailTableScan::class.java, none()))), "CtLimitOnProject") {

    /**
     * This rule only matches, of the projected fields are all actual columns found in the [Entity]
     *
     * @param call The [RelOptRuleCall] to check.
     */
    override fun matches(call: RelOptRuleCall): Boolean {
        val project = call.rel<CottontailProject>(1)
        for (e in project.projects) {
            if (e !is RexInputRef) {
                return false
            }
        }
        return true
    }

    /**
     * Merges the [EnumerableLimit] and [CottontailProject] clause into single instance of [CottontailTableScan].
     *
     * @param call The [RelOptRuleCall]
     */
    override fun onMatch(call: RelOptRuleCall) {
        val limit = call.rel<CottontailLimit>(0)
        val project = call.rel<CottontailProject>(1)
        val table = call.rel<CottontailTableScan>(2)
        val traitSet = limit.traitSet.replace(CottontailRel.CONVENTION)
        call.transformTo(CottontailTableScan(limit.cluster, traitSet, table.table, table.cottontailTable, project, limit.offset, limit.limit))
    }
}