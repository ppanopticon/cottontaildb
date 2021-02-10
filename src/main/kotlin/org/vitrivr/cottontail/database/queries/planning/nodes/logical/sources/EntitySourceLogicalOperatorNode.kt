package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.NullaryLogicalOperatorNode

/**
 * A [NullaryLogicalOperatorNode] that formalizes accessing data from an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
abstract class EntitySourceLogicalOperatorNode(
    val entity: Entity,
    override val columns: Array<ColumnDef<*>>
) : NullaryLogicalOperatorNode() {
    /**
     * Calculates and returns the digest for this [EntitySourceLogicalOperatorNode].
     *
     * @return Digest for this [EntitySourceLogicalOperatorNode]
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.entity.hashCode()
        result = 31L * result + this.columns.contentHashCode()
        return result
    }
}