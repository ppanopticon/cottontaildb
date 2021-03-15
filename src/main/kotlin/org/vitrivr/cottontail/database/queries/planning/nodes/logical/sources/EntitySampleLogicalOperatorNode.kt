package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.NullaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntitySamplePhysicalOperatorNode

/**
 * A [NullaryLogicalOperatorNode] that formalizes the sampling of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class EntitySampleLogicalOperatorNode(
    override val groupId: Int,
    val entity: Entity,
    override val columns: Array<ColumnDef<*>>,
    val size: Long,
    val seed: Long = System.currentTimeMillis()
) : NullaryLogicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "SampleEntity"
    }

    /** The name of this [EntitySampleLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /**
     * Creates and returns a copy of this [EntitySampleLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [EntitySampleLogicalOperatorNode].
     */
    override fun copy() = EntitySampleLogicalOperatorNode(this.groupId, this.entity, this.columns, this.size, this.seed)

    /**
     * Returns a [EntitySamplePhysicalOperatorNode] representation of this [EntitySampleLogicalOperatorNode]
     *
     * @return [EntitySamplePhysicalOperatorNode]
     */
    override fun implement(): Physical = EntitySamplePhysicalOperatorNode(this.groupId, this.entity, this.columns, this.size, this.seed)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is EntitySampleLogicalOperatorNode) return false
        if (!super.equals(other)) return false

        if (entity != other.entity) return false
        if (!columns.contentEquals(other.columns)) return false
        if (size != other.size) return false
        if (seed != other.seed) return false

        return true
    }

    /** Generates and returns a hash code for this [EntitySampleLogicalOperatorNode]. */
    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + columns.contentHashCode()
        result = 31 * result + size.hashCode()
        result = 31 * result + seed.hashCode()
        return result
    }

    /** Generates and returns a [String] representation of this [EntitySampleLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }}]"
}