package ch.unibas.dmi.dbis.cottontail.calcite.rel

import ch.unibas.dmi.dbis.cottontail.calcite.adapter.CottontailTable
import ch.unibas.dmi.dbis.cottontail.calcite.rules.merge.CottontailLimitOnScanRule
import ch.unibas.dmi.dbis.cottontail.calcite.rules.convert.CottontailProjectConvertRule
import ch.unibas.dmi.dbis.cottontail.calcite.rules.CottontailToEnumerableConverterRule
import ch.unibas.dmi.dbis.cottontail.calcite.rules.RexToCottontailTranslator
import ch.unibas.dmi.dbis.cottontail.calcite.rules.convert.CottontailFilterConvertRule
import ch.unibas.dmi.dbis.cottontail.calcite.rules.convert.CottontailLimitConvertRule
import ch.unibas.dmi.dbis.cottontail.calcite.rules.merge.CottontailLimitOnProjectRule
import ch.unibas.dmi.dbis.cottontail.calcite.rules.merge.CottontailProjectOnScan
import org.apache.calcite.adapter.java.JavaTypeFactory

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelTraitSet

import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.RelNode

import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.validate.SqlValidatorUtil


/**
 * A relational expression representing a full scan of a Cottontail DB [Entity] and implements [TableScan].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class CottontailTableScan(
        cluster: RelOptCluster,
        traitSet: RelTraitSet,
        table: RelOptTable,
        val cottontailTable: CottontailTable,
        val project: CottontailProject? = null,
        val offset: RexNode? = null,
        val limit: RexNode? = null)
    : TableScan(cluster, traitSet, table), CottontailRel {

    init {
        /* Some basic sanity check. */
        assert(convention === CottontailRel.CONVENTION)
    }

    /**
     * Copies this [CottontailTableScan] by returning the same instance
     *
     * @param traitSet
     * @param input
     * @return this
     */
    override fun copy(traitSet: RelTraitSet, c: List<RelNode>): RelNode = CottontailTableScan(cluster, traitSet, this.table, this.cottontailTable, this.project, this.offset, this.limit)

    /**
     * Returns the [RelDataType] returned by  this [CottontailTableScan] instance. If a [CottontailProject] clause
     * has been specified, that clause determines the type. Otherwise, the type is determined by the table.
     *
     * @return The [RelDataType] returned by this [CottontailTableScan].
     */
    override fun deriveRowType(): RelDataType = this.project?.rowType ?: this.table.rowType

    /**
     * Registers all [RelOptRule]s associated with this [CottontailTableScan].
     *
     * @param planner The [RelOptPlanner] instance responsible for laying out the query plan.
     */
    override fun register(planner: RelOptPlanner) {

        /* Converter rules. */
        planner.addRule(CottontailLimitConvertRule)
        planner.addRule(CottontailProjectConvertRule)
        planner.addRule(CottontailFilterConvertRule)

        /* Optimization rules. */
        planner.addRule(CottontailProjectOnScan)
        planner.addRule(CottontailLimitOnScanRule)
        planner.addRule(CottontailLimitOnProjectRule)
        planner.addRule(CottontailToEnumerableConverterRule.INSTANCE)
        super.register(planner)
    }

    /**
     * Applies all the different clauses to the [CottontailRel.Implementor].
     *
     * @param implementor [CottontailRel.Implementor] instance.
     */
    override fun implement(implementor: CottontailRel.Implementor) {
        /* Apply basic TABLE properties for SCAN. */
        implementor.cottontailTable = cottontailTable
        implementor.table = table

        /* Set PROJECTION clause. */
        if (this.project != null) {
            val fieldNames = SqlValidatorUtil.uniquify(this.project.input.rowType.fieldNames, SqlValidatorUtil.EXPR_SUGGESTER, true)
            val translator = RexToCottontailTranslator(this.project.cluster.typeFactory as JavaTypeFactory, fieldNames)

            this.project.namedProjects.forEach {
                val name = it.left.accept(translator)
                implementor.addProjection(name, it.right)
            }
        }

        /* Set OFFSET and LIMIT clauses. */
        if (offset != null) {
            implementor.offset = (RexLiteral.value(offset) as Number).toLong()
        }
        if (limit != null) {
            implementor.limit = (RexLiteral.value(limit) as Number).toLong()
        }
    }
}