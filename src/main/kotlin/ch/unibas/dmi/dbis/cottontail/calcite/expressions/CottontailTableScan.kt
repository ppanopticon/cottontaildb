package ch.unibas.dmi.dbis.cottontail.calcite.expressions

import ch.unibas.dmi.dbis.cottontail.calcite.adapter.CottontailTable
import ch.unibas.dmi.dbis.cottontail.calcite.rules.ProjectTableScanRule

import org.apache.calcite.adapter.enumerable.*
import org.apache.calcite.linq4j.tree.Blocks
import org.apache.calcite.linq4j.tree.Expressions
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.RelNode
import org.apache.calcite.plan.RelTraitSet

/**
 *
 */
internal class CottontailTableScan (cluster: RelOptCluster, table: RelOptTable, private val fieldList: List<RelDataTypeField>, val entity: CottontailTable) : TableScan(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), table), EnumerableRel {

    /**
     * Copies this instance of [CottontailTableScan] and returns the copy.
     *
     * @param traitSet
     * @param inputs
     * @return Copy of this [CottontailTableScan].
     */
    override fun copy(traitSet: RelTraitSet?, inputs: List<RelNode>): RelNode {
        assert(inputs.isEmpty())
        return CottontailTableScan(this.cluster, this.table, this.fieldList,  this.entity)
    }

    /**
     *
     */
    override fun explainTerms(pw: RelWriter): RelWriter {
        return super.explainTerms(pw).item("fields", this.fieldList)
    }

    /**
     * Generates and returns the [RelDataType] describing the row returned by this instance of [CottontailTableScan].
     *
     * @return The [RelDataType] that describes the row returned by this [CottontailTableScan].
     */
    override fun deriveRowType(): RelDataType = this.cluster.typeFactory.builder().let { builder ->
        this.fieldList.forEach { builder.add(it) }
        builder.build()
    }

    /**
     * Registers the [ProjectTableScanRule]
     */
    override fun register(planner: RelOptPlanner) {
        planner.addRule(ProjectTableScanRule)
    }

    /**
     *
     */
    override fun implement(implementor: EnumerableRelImplementor, pref: EnumerableRel.Prefer): EnumerableRel.Result {
        val physType = PhysTypeImpl.of(implementor.typeFactory, getRowType(), pref.preferArray())
        val fields = this.fieldList.map { it.name }.toTypedArray()
        return implementor.result(
                physType,
                Blocks.toBlock(
                        Expressions.call(this.table.getExpression(CottontailTable::class.java), "project", implementor.rootExpression, Expressions.constant(fields))))
    }
}