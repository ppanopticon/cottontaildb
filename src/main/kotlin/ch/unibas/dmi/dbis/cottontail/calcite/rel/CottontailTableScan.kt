package ch.unibas.dmi.dbis.cottontail.calcite.rel

import ch.unibas.dmi.dbis.cottontail.calcite.adapter.CottontailTable
import ch.unibas.dmi.dbis.cottontail.calcite.rules.convert.CottontailLimitRule
import ch.unibas.dmi.dbis.cottontail.calcite.rules.convert.CottontailProjectRule
import ch.unibas.dmi.dbis.cottontail.calcite.rules.CottontailToEnumerableConverterRule

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelTraitSet

import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.RelNode

import org.apache.calcite.plan.RelOptPlanner


/**
 * A relational expression representing a full scan of a Cottontail DB [Entity] and implements [TableScan].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class CottontailTableScan(cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, val cottontailTable: CottontailTable, val projectRowType: RelDataType? = null) : TableScan(cluster, traitSet, table), CottontailRel {

    init {
        assert(convention === CottontailRel.CONVENTION)
    }

    /**
     * Copies this [CottontailTableScan] by returning the same instance
     *
     * @param traitSet
     * @param input
     * @return this
     */
    override fun copy(traitSet: RelTraitSet, c: List<RelNode>): RelNode {
        assert(inputs.isEmpty())
        return this
    }

    /**
     *
     */
    override fun deriveRowType(): RelDataType = projectRowType ?: super.deriveRowType()

    /**
     *
     */
    override fun register(planner: RelOptPlanner) {
        planner.addRule(CottontailProjectRule)
        planner.addRule(CottontailLimitRule)
        planner.addRule(CottontailToEnumerableConverterRule.INSTANCE)
        super.register(planner)
    }

    /**
     *
     */
    override fun implement(implementor: CottontailRel.Implementor) {
        implementor.cottontailTable = cottontailTable
        implementor.table = table
    }
}