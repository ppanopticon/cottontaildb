package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [EntitySourceLogicalNodeExpression] that formalizes the scan of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class EntityScanLogicalNodeExpression(entity: Entity, columns: Array<ColumnDef<*>> = entity.allColumns().toTypedArray()) : EntitySourceLogicalNodeExpression(entity, columns) {
    /**
     * Returns a copy of this [EntityScanLogicalNodeExpression]
     *
     * @return Copy of this [EntityScanLogicalNodeExpression]
     */
    override fun copy(): EntityScanLogicalNodeExpression = EntityScanLogicalNodeExpression(this.entity, this.columns)
}