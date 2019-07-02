package ch.unibas.dmi.dbis.cottontail.calcite.rel

import ch.unibas.dmi.dbis.cottontail.calcite.rules.RexToCottontailTranslator
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException



import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Project
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
     *
     */
    override fun implement(implementor: CottontailRel.Implementor) {
        implementor.visitChild(0, getInput())

        val fieldNames = SqlValidatorUtil.uniquify(getInput().rowType.fieldNames, SqlValidatorUtil.EXPR_SUGGESTER, true)
        val translator = RexToCottontailTranslator(this.cluster.typeFactory as JavaTypeFactory, fieldNames)

        val table = implementor.cottontailTable
        if (table != null) {
            this.namedProjects.forEach {
                val name = it.left.accept(translator)
                implementor.addProjection(table.source.columnForName(name) ?: throw QueryException.QueryBindException("Failed to bind column '$name' to a column in entity ${table.source.fqn}."), it.right)
            }
        }
    }
}