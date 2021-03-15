package org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection

import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.AbstractProjectionLogicalOperatorOperator
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.transform.DistanceLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.model.basics.Name

/**
 * An abstract [UnaryPhysicalOperatorNode] that represents a projection operation involving.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
abstract class AbstractProjectionPhysicalOperatorNode(input: Physical? = null, val type: Projection, val fields: List<Pair<Name.ColumnName, Name.ColumnName?>>) : UnaryPhysicalOperatorNode(input) {

    /** The name of this [DistanceLogicalOperatorNode]. */
    override val name: String
        get() = this.type.label()

    /**
     * Compares this [AbstractProjectionPhysicalOperatorNode] to another object.
     *
     * @param other The other [Any] to compare this [AbstractProjectionPhysicalOperatorNode] to.
     * @return True if other equals this, false otherwise.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is AbstractProjectionLogicalOperatorOperator) return false

        if (this.type != other.type) return false
        if (this.fields != other.fields) return false

        return true
    }

    /** Generates and returns a [String] representation of this [AbstractProjectionPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }}]"

    /**
     * Generates and returns a hash code for this [AbstractProjectionPhysicalOperatorNode].
     */
    override fun hashCode(): Int {
        var result = this.type.hashCode()
        result = 31 * result + this.fields.hashCode()
        return result
    }
}