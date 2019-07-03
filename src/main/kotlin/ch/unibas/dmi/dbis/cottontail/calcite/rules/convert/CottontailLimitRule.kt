package ch.unibas.dmi.dbis.cottontail.calcite.rules.convert

import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailLimit
import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailRel
import ch.unibas.dmi.dbis.cottontail.calcite.rules.CottontailToEnumerableConverter

import org.apache.calcite.adapter.enumerable.EnumerableLimit

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelOptRule

/**
 * Rule to convert a [org.apache.calcite.adapter.enumerable.EnumerableLimit] to a [CottontailLimit].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal object CottontailLimitRule : RelOptRule(operand(EnumerableLimit::class.java, operand(CottontailToEnumerableConverter::class.java, any())), "CtLimit") {


    override fun matches(call: RelOptRuleCall): Boolean {
        return true
    }

    /**
     *
     */
    override fun onMatch(call: RelOptRuleCall) {
        val limit = call.rel<EnumerableLimit>(0)
        val traitSet = limit.traitSet.replace(CottontailRel.CONVENTION)
        call.transformTo(CottontailLimit(limit.cluster, traitSet, convert(limit.input, CottontailRel.CONVENTION), limit.offset, limit.fetch))
    }
}







