package ch.unibas.dmi.dbis.cottontail.calcite.rules.convert

import ch.unibas.dmi.dbis.cottontail.calcite.adapter.toPredicate
import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailFilter

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.calcite.rex.RexCall

object CottontailFilterConvertRule : CottontailConverterRule<LogicalFilter>(LogicalFilter::class.java, "CtFilter") {

    /**
     * This rule only matches, of the projected fields are all actual columns found in the [Entity]
     *
     * @param call The [RelOptRuleCall] to check.
     */
    override fun matches(call: RelOptRuleCall): Boolean {
        val filter = call.rel<LogicalFilter>(0)
        val condition = filter.condition
        if (condition is RexCall) { try {
                condition.toPredicate(filter)
            } catch (e: Exception) {
                return false
            }
            return true
        } else {
            return false
        }
    }

    /**
     *
     */
    override fun convert(rel: RelNode): RelNode {
        val filter = rel as LogicalFilter
        val traitSet = filter.traitSet.replace(this.out)
        return CottontailFilter(filter.cluster, traitSet, RelOptRule.convert(filter.input, this.out), filter.condition, (filter.condition as RexCall).toPredicate(filter))
    }
}