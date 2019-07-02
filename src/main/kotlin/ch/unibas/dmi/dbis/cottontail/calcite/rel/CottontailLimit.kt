package ch.unibas.dmi.dbis.cottontail.calcite.rel

import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.rel.RelNode
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.plan.RelOptCost
import org.apache.calcite.rex.RexNode
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.rel.AbstractRelNode
import org.apache.calcite.rel.SingleRel
import org.apache.calcite.rel.core.Project


/**
 * A relational expression representing LIMIT and OFFSET the Cottontail DB [Entity] and implements [Project].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class CottontailLimit(cluster: RelOptCluster, traitSet: RelTraitSet, input: RelNode, val offset: RexNode?, val limit: RexNode?) : SingleRel(cluster, traitSet, input), CottontailRel {

    init {
        assert(convention === input.convention)
    }

    /**
     *
     */
    override fun computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost {
        return planner.costFactory.makeZeroCost()
    }

    /**
     *
     */
    override fun copy(traitSet: RelTraitSet, newInputs: List<RelNode>): CottontailLimit {
        return CottontailLimit(cluster, traitSet, AbstractRelNode.sole(newInputs), offset, limit)
    }

    /**
     *
     */
    override fun implement(implementor: CottontailRel.Implementor) {
        implementor.visitChild(0, getInput())
        if (offset != null) {
            implementor.offset = (RexLiteral.value(offset) as Number).toLong()
        }
        if (limit != null) {
            implementor.limit = (RexLiteral.value(limit) as Number).toLong()
        }
    }

    /**
     *
     */
    override fun explainTerms(pw: RelWriter): RelWriter {
        super.explainTerms(pw)
        pw.itemIf("offset", offset, offset != null)
        pw.itemIf("limit", limit, limit != null)
        return pw
    }
}