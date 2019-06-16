package ch.unibas.dmi.dbis.cottontail.calcite.rules

import ch.unibas.dmi.dbis.cottontail.calcite.expressions.CottontailTableScan
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.calcite.rel.logical.LogicalProject

object FilterTableScanRule: RelOptRule(operand(LogicalFilter::class.java, operand(CottontailTableScan::class.java, none())), RelFactories.LOGICAL_BUILDER, "CottontailProjectTableScanRule") {

    /**
     *
     */
    override fun onMatch(call: RelOptRuleCall) {
        val filtrr: LogicalFilter = call.rel(0)
        val scan : CottontailTableScan = call.rel(1)
    }
}