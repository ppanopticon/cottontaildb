package ch.unibas.dmi.dbis.cottontail.calcite.rules.merge

import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailLimit
import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailProject
import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailRel
import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailTableScan

import org.apache.calcite.adapter.enumerable.EnumerableLimit
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall

/**
 * Rule to convert a [org.apache.calcite.adapter.enumerable.EnumerableLimit] to a [CottontailLimit].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal object CottontailProjectOnScan : RelOptRule(operand(CottontailProject::class.java, operand(CottontailTableScan::class.java, none())), "CtProjectOnScan") {
    /**
     * Merges the the given [EnumerableLimit] to a [CottontailLimit].
     *
     * @param rel The [EnumerableLimit] into a [CottontailTableScan].
     */
    override fun onMatch(call: RelOptRuleCall) {
        val project = call.rel<CottontailProject>(0)
        val scan = call.rel<CottontailTableScan>(1)
        val traitSet = project.traitSet.replace(CottontailRel.CONVENTION)
        call.transformTo(CottontailTableScan(project.cluster, traitSet, scan.table, scan.cottontailTable, project, null, null))
    }
}
