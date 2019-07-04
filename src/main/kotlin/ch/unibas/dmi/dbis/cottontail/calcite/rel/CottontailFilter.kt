package ch.unibas.dmi.dbis.cottontail.calcite.rel

import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rex.RexNode



/**
 *
 */
internal class CottontailFilter(
        cluster: RelOptCluster,
        traitSet: RelTraitSet,
        child: RelNode,
        condition: RexNode,
        predicate: Predicate)
    : Filter(cluster,  traitSet, child, condition), CottontailRel {


    init {
        assert(convention === CottontailRel.CONVENTION)
        assert(convention === child.convention)
    }

    override fun copy(traitSet: RelTraitSet?, input: RelNode?, condition: RexNode?): Filter {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun implement(implementor: CottontailRel.Implementor) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}