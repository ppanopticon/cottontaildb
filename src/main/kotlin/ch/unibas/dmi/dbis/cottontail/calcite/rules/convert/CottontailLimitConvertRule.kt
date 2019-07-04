package ch.unibas.dmi.dbis.cottontail.calcite.rules.convert

import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailLimit
import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailRel
import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailTableScan
import ch.unibas.dmi.dbis.cottontail.calcite.rules.CottontailToEnumerableConverter
import org.apache.calcite.adapter.enumerable.EnumerableLimit
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall

/**
 * Converter rule to convert a [EnumerableLimit] to a [CottontailLimit].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal object CottontailLimitConvertRule : RelOptRule(operand(EnumerableLimit::class.java, operand(CottontailToEnumerableConverter::class.java, any())), "EnumerableLimitConversion") {

    /**
     * This rule only matches, of the projected fields are all actual columns found in the [Entity]
     *
     * @param call The [RelOptRuleCall] to check.
     */
    override fun matches(call: RelOptRuleCall): Boolean {
        return true
    }

    /**
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