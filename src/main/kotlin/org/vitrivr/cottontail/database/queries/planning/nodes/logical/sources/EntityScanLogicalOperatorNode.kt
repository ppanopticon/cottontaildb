package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.NullaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntityScanPhysicalOperatorNode

/**
 * A [NullaryLogicalOperatorNode] that formalizes the scan of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.1.1
 */
class EntityScanLogicalOperatorNode(override val groupId: Int, val entity: EntityTx, override val columns: Array<ColumnDef<*>>) : NullaryLogicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "ScanEntity"
    }

    /** The name of this [EntityScanLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /**
     * Creates and returns a copy of this [EntityScanLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [EntityScanLogicalOperatorNode].
     */
    override fun copy() = EntityScanLogicalOperatorNode(this.groupId, this.entity, this.columns)

    /**
     * Returns a [EntityScanPhysicalOperatorNode] representation of this [EntityScanLogicalOperatorNode]
     *
     * @return [EntityScanPhysicalOperatorNode]
     */
    override fun implement(): Physical = EntityScanPhysicalOperatorNode(this.groupId, this.entity, this.columns)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is EntityScanLogicalOperatorNode) return false

        if (entity != other.entity) return false
        if (!columns.contentEquals(other.columns)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + columns.contentHashCode()
        return result
    }

    /** Generates and returns a [String] representation of this [EntitySampleLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }}]"
}