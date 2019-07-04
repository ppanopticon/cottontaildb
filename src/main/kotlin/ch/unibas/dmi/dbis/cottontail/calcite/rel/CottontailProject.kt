package ch.unibas.dmi.dbis.cottontail.calcite.rel

import ch.unibas.dmi.dbis.cottontail.calcite.rules.RexToCottontailTranslator


import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptCost
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.validate.SqlValidatorUtil

/**
 * A relational expression representing a projection of the Cottontail DB [Entity] and implements [Project].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class CottontailProject(cluster: RelOptCluster, traitSet: RelTraitSet, input: RelNode, projects: MutableList<RexNode>, rowType: RelDataType) : Project(cluster, traitSet, input, projects, rowType), CottontailRel {

    init {
        assert(convention === CottontailRel.CONVENTION)
        assert(convention === input.convention)
    }

    /**
     *
     */
    override fun copy(traitSet: RelTraitSet, input: RelNode, projects: MutableList<RexNode>, rowType: RelDataType): Project = CottontailProject(cluster, traitSet, input, projects, rowType)

    /**
     * Computes the costs of applying a [CottontailProject] vs. normal projection as executed by Apache Calcite.
     *
     * @param planner The [RelOptPlanner]
     * @param mq The [RelMetadataQuery]
     */
    override fun computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost {
        return planner.costFactory.makeZeroCost()
    }

    /**
     *
     */
    override fun implement(implementor: CottontailRel.Implementor) {
        implementor.visitChild(0, getInput())

        /* Apply PROJECTION. */
        val fieldNames = SqlValidatorUtil.uniquify(getInput().rowType.fieldNames, SqlValidatorUtil.EXPR_SUGGESTER, true)
        val translator = RexToCottontailTranslator(this.cluster.typeFactory as JavaTypeFactory, fieldNames)
        val table = implementor.cottontailTable
        if (table != null) {
            this.namedProjects.forEach {
                val name = it.left.accept(translator)
                implementor.addProjection(name, it.right)
            }
        }
    }
}