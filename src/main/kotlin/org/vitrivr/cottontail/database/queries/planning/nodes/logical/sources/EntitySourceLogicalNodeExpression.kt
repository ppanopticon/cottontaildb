package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.NullaryLogicalNodeExpression
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * A [NullaryLogicalNodeExpression] that formalizes accessing data from an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
abstract class EntitySourceLogicalNodeExpression(
    val entity: Entity,
    override val columns: Array<ColumnDef<*>>
) : NullaryLogicalNodeExpression() {
    /**
     * Calculates and returns the digest for this [EntitySourceLogicalNodeExpression].
     *
     * @return Digest for this [EntitySourceLogicalNodeExpression]
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.entity.hashCode()
        result = 31L * result + this.columns.contentHashCode()
        return result
    }
}