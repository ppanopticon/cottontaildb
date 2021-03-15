package org.vitrivr.cottontail.database.queries.planning.nodes.logical.management

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.NullaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.management.InsertPhysicalOperatorNode
import org.vitrivr.cottontail.execution.operators.management.InsertOperator
import org.vitrivr.cottontail.model.basics.Record

/**
 * A [InsertLogicalOperatorNode] that formalizes a INSERT operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class InsertLogicalOperatorNode(override val groupId: GroupId, val entity: Entity, val records: MutableList<Binding<Record>>) : NullaryLogicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "Insert"
    }

    /** The name of this [InsertLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [InsertLogicalOperatorNode] produces the columns defined in the [InsertOperator] */
    override val columns: Array<ColumnDef<*>> = InsertOperator.COLUMNS

    /**
     * Creates and returns a copy of this [InsertLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [InsertLogicalOperatorNode].
     */
    override fun copy() = InsertLogicalOperatorNode(this.groupId, this.entity, this.records)

    /**
     * Returns a [InsertPhysicalOperatorNode] representation of this [InsertLogicalOperatorNode]
     *
     * @return [InsertPhysicalOperatorNode]
     */
    override fun implement() = InsertPhysicalOperatorNode(this.groupId, this.entity, this.records)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is InsertLogicalOperatorNode) return false

        if (entity != other.entity) return false
        if (records != other.records) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + records.hashCode()
        return result
    }
}