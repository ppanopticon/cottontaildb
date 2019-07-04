package ch.unibas.dmi.dbis.cottontail.calcite.rules.convert

import ch.unibas.dmi.dbis.cottontail.calcite.adapter.toPredicate
import ch.unibas.dmi.dbis.cottontail.calcite.rel.CottontailFilter
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalFilter

object CottontailFilterConvertRule : CottontailConverterRule<LogicalFilter>(LogicalFilter::class.java, "CtFilter") {


    /**
     *
     */
    override fun convert(rel: RelNode): RelNode {
        val filter = rel as LogicalFilter
        val traitSet = filter.traitSet.replace(this.out)
        return CottontailFilter(filter.cluster, traitSet, RelOptRule.convert(filter.input, this.out), filter.condition, filter.condition.toPredicate(filter))
    }
}