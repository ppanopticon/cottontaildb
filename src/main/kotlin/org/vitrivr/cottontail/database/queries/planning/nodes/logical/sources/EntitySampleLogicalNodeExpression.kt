package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.NullaryLogicalNodeExpression
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [NullaryLogicalNodeExpression] that formalizes the sampling of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class EntitySampleLogicalNodeExpression(val entity: Entity, val columns: Array<ColumnDef<*>> = entity.allColumns().toTypedArray(), val size: Long, val seed: Long = System.currentTimeMillis()) : NullaryLogicalNodeExpression() {
    /**
     * Returns a copy of this [EntitySampleLogicalNodeExpression]
     *
     * @return Copy of this [EntitySampleLogicalNodeExpression]
     */
    override fun copy(): EntitySampleLogicalNodeExpression = EntitySampleLogicalNodeExpression(this.entity, this.columns, this.size, this.seed)
}