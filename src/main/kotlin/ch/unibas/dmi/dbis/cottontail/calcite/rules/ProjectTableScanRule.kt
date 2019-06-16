package ch.unibas.dmi.dbis.cottontail.calcite.rules

import ch.unibas.dmi.dbis.cottontail.calcite.expressions.CottontailTableScan
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.tools.RelBuilderFactory

/**
 * A simple rule that removes field that are not included in the projection from the [CottontailTableScan]. Since Cottontail DB
 * is a column store, doing so significantly reduces IO.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object ProjectTableScanRule: RelOptRule(operand(LogicalProject::class.java, operand(CottontailTableScan::class.java, none())), RelFactories.LOGICAL_BUILDER, "CottontailProjectTableScanRule") {
    override fun onMatch(call: RelOptRuleCall) {
        val project: LogicalProject = call.rel(0)
        val scan : CottontailTableScan = call.rel(1)
        call.transformTo(CottontailTableScan(scan.cluster, scan.table, project.rowType.fieldList, scan.entity))
    }
}