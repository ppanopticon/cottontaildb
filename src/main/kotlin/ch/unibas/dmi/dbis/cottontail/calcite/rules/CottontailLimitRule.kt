package ch.unibas.dmi.dbis.cottontail.calcite.rules

import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailLimit
import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailRel

import org.apache.calcite.rel.RelNode

import org.apache.calcite.adapter.enumerable.EnumerableLimit

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelOptRule


/**
 * Rule to convert a [org.apache.calcite.adapter.enumerable.EnumerableLimit] to a
 * [CassandraLimit].
 */
internal object CottontailLimitRule : RelOptRule(operand(EnumerableLimit::class.java, operand(CottontailToEnumerableConverter::class.java, any())), "CottontailLimitRule") {

    /** @see org.apache.calcite.rel.convert.ConverterRule
     */
    override fun onMatch(call: RelOptRuleCall) {
        val limit = call.rel<EnumerableLimit>(0)
        val converted = convert(limit)
        call.transformTo(converted)
    }

    fun convert(limit: EnumerableLimit): RelNode {
        val traitSet = limit.traitSet.replace(CottontailRel.CONVENTION)
        return CottontailLimit(limit.cluster, traitSet, convert(limit.input, CottontailRel.CONVENTION), limit.offset, limit.fetch)
    }
}