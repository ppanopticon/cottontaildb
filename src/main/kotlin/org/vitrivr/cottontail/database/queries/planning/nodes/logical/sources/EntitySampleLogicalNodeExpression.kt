package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [EntitySourceLogicalNodeExpression] that formalizes the sampling of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.2
 */
class EntitySampleLogicalNodeExpression(
    entity: Entity,
    columns: Array<ColumnDef<*>>,
    val size: Long,
    val seed: Long = System.currentTimeMillis()
) : EntitySourceLogicalNodeExpression(entity, columns) {
    /**
     * Returns a copy of this [EntitySampleLogicalNodeExpression]
     *
     * @return Copy of this [EntitySampleLogicalNodeExpression]
     */
    override fun copy(): EntitySampleLogicalNodeExpression =
        EntitySampleLogicalNodeExpression(this.entity, this.columns, this.size, this.seed)

    /**
     * Calculates and returns the digest for this [EntitySampleLogicalNodeExpression].
     *
     * @return Digest for this [EntitySampleLogicalNodeExpression]
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.size.hashCode()
        result = 31L * result + this.seed.hashCode()
        return result
    }
}