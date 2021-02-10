package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity

/**
 * A [EntitySourceLogicalOperatorNode] that formalizes the sampling of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.2
 */
class EntitySampleLogicalOperatorNode(
    entity: Entity,
    columns: Array<ColumnDef<*>>,
    val size: Long,
    val seed: Long = System.currentTimeMillis()
) : EntitySourceLogicalOperatorNode(entity, columns) {
    /**
     * Returns a copy of this [EntitySampleLogicalOperatorNode]
     *
     * @return Copy of this [EntitySampleLogicalOperatorNode]
     */
    override fun copy(): EntitySampleLogicalOperatorNode =
        EntitySampleLogicalOperatorNode(this.entity, this.columns, this.size, this.seed)

    /**
     * Calculates and returns the digest for this [EntitySampleLogicalOperatorNode].
     *
     * @return Digest for this [EntitySampleLogicalOperatorNode]
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.size.hashCode()
        result = 31L * result + this.seed.hashCode()
        return result
    }
}