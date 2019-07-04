package ch.unibas.dmi.dbis.cottontail.calcite.rules.merge

import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailLimit
import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailProject
import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailRel
import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailTableScan
import ch.unibas.dmi.dbis.cottontail.calcite.rules.CottontailToEnumerableConverter

import org.apache.calcite.adapter.enumerable.EnumerableLimit

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rex.RexInputRef

/**
 * Rule to convert a [org.apache.calcite.adapter.enumerable.EnumerableLimit] to a [CottontailLimit].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal object CottontailLimitOnScanRule : RelOptRule(operand(CottontailLimit::class.java, operand(CottontailTableScan::class.java, any())), "CtLimitOnScan") {
    /*
     * Merges the the given [EnumerableLimit] to a [CottontailLimit].
     *
     * @param rel The [EnumerableLimit] into a [CottontailTableScan].
     */
    override fun onMatch(call: RelOptRuleCall) {
        val limit = call.rel<CottontailLimit>(0)
        val table = call.rel<CottontailTableScan>(1)
        val traitSet = limit.traitSet.replace(CottontailRel.CONVENTION)
        call.transformTo(CottontailTableScan(limit.cluster, traitSet, table.table, table.cottontailTable, null, limit.offset, limit.limit))
    }
}







