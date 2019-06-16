package ch.unibas.dmi.dbis.cottontail.calcite.expressions

import ch.unibas.dmi.dbis.cottontail.calcite.adapter.CottontailTable
import ch.unibas.dmi.dbis.cottontail.calcite.rules.ProjectTableScanRule
import org.apache.calcite.adapter.enumerable.EnumerableConvention
import org.apache.calcite.adapter.enumerable.EnumerableRel
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.type.RelDataType

/**
 *
 */
internal class CottontailFilterScan (cluster: RelOptCluster, table: RelOptTable, val entity: CottontailTable) : TableScan(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), table), EnumerableRel {

    /**
     * Generates and returns the [RelDataType] describing the row returned by this instance of [CottontailTableScan].
     *
     * @return The [RelDataType] that describes the row returned by this [CottontailTableScan].
     */
    override fun deriveRowType(): RelDataType = this.cluster.typeFactory.builder().let { builder ->
        this.table.rowType.fieldList.forEach { builder.add(it) }
        builder.build()
    }

    /**
     * Registers the [ProjectTableScanRule]
     */
    override fun register(planner: RelOptPlanner) {
        planner.addRule(ProjectTableScanRule)
    }

    override fun implement(implementor: EnumerableRelImplementor?, pref: EnumerableRel.Prefer?): EnumerableRel.Result {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}