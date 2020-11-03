package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.NullaryLogicalNodeExpression
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [NullaryLogicalNodeExpression] that formalizes the scan of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class EntityScanLogicalNodeExpression(val entity: Entity, val columns: Array<ColumnDef<*>> = entity.allColumns().toTypedArray()): NullaryLogicalNodeExpression() {

    /**
     * Returns a copy of this [EntityScanLogicalNodeExpression]
     *
     * @return Copy of this [EntityScanLogicalNodeExpression]
     */
    override fun copy(): EntityScanLogicalNodeExpression = EntityScanLogicalNodeExpression(this.entity, this.columns)
}