package org.vitrivr.cottontail.database.queries.planning.nodes.logical.management

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.management.UpdatePhysicalOperatorNode
import org.vitrivr.cottontail.execution.operators.management.UpdateOperator
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [DeleteLogicalOperatorNode] that formalizes an UPDATE operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class UpdateLogicalOperatorNode(input: Logical? = null, val entity: Entity, val values: List<Pair<ColumnDef<*>, Binding<Value>>>) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Update"
    }

    /** The name of this [InsertLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [UpdateLogicalOperatorNode] does produce the columns defined in the [UpdateOperator]. */
    override val columns: Array<ColumnDef<*>> = UpdateOperator.COLUMNS

    /**
     * Creates and returns a copy of this [UpdateLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [UpdateLogicalOperatorNode].
     */
    override fun copy() = UpdateLogicalOperatorNode(entity = this.entity, values = this.values)

    /**
     * Returns a [UpdatePhysicalOperatorNode] representation of this [UpdateLogicalOperatorNode]
     *
     * @return [UpdatePhysicalOperatorNode]
     */
    override fun implement() = UpdatePhysicalOperatorNode(this.input?.implement(), this.entity, this.values)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is UpdateLogicalOperatorNode) return false

        if (entity != other.entity) return false
        if (values != other.values) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + values.hashCode()
        return result
    }
}