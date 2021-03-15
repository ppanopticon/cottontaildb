package org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection

import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.transform.DistanceLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.model.basics.Name

/**
 * An abstract [UnaryLogicalOperatorNode] that represents a projection operation involving.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
abstract class AbstractProjectionLogicalOperatorOperator(input: Logical? = null, val type: Projection, val fields: List<Pair<Name.ColumnName, Name.ColumnName?>>) : UnaryLogicalOperatorNode(input) {
    /** The name of this [DistanceLogicalOperatorNode]. */
    override val name: String
        get() = this.type.label()

    /**
     * Compares this [AbstractProjectionLogicalOperatorOperator] to another object.
     *
     * @param other The other [Any] to compare this [AbstractProjectionLogicalOperatorOperator] to.
     * @return True if other equals this, false otherwise.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is AbstractProjectionLogicalOperatorOperator) return false

        if (this.type != other.type) return false
        if (this.fields != other.fields) return false

        return true
    }

    /**
     * Generates and returns a hash code for this [AbstractProjectionLogicalOperatorOperator].
     */
    override fun hashCode(): Int {
        var result = this.type.hashCode()
        result = 31 * result + this.fields.hashCode()
        return result
    }

    /** Generates and returns a [String] representation of this [SelectProjectionLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }}]"
}