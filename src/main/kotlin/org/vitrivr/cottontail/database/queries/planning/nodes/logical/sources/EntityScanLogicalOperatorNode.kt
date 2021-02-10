package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity

/**
 * A [EntitySourceLogicalOperatorNode] that formalizes the scan of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.2
 */
class EntityScanLogicalOperatorNode(entity: Entity, columns: Array<ColumnDef<*>>) :
    EntitySourceLogicalOperatorNode(entity, columns) {
    /**
     * Returns a copy of this [EntityScanLogicalOperatorNode]
     *
     * @return Copy of this [EntityScanLogicalOperatorNode]
     */
    override fun copy(): EntityScanLogicalOperatorNode =
        EntityScanLogicalOperatorNode(this.entity, this.columns)
}