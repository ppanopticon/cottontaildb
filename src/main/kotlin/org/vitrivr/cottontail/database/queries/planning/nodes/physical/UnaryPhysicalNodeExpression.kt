package org.vitrivr.cottontail.database.queries.planning.nodes.physical

/**
 * An abstract [PhysicalNodeExpression] implementation that has a single [NodeExpression] as input.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class UnaryPhysicalNodeExpression : PhysicalNodeExpression() {
    /** The arity of the [UnaryPhysicalNodeExpression] is always on. */
    override val inputArity = 1

    /** [UnaryPhysicalNodeExpression] can be partitioned, if its parent can be partitioned. */
    override val canBePartitioned: Boolean
        get() = this.input.canBePartitioned

    /**
     * Tries to create [p] partitions of this [UnaryPhysicalNodeExpression], which is possible
     * as long as all upstream [PhysicalNodeExpression]s can be partitioned.
     *
     * @param p The desired number of partitions.
     * @return Array of [PhysicalNodeExpression]s.
     *
     * @throws IllegalStateException If this [PhysicalNodeExpression] cannot be partitioned.
     */
    override fun partition(p: Int): List<PhysicalNodeExpression> = this.input.partition(p).map {
        val copy = this.copy()
        copy.addInput(it)
        copy
    }

    /** Reference to the input [PhysicalNodeExpression]. */
    val input: PhysicalNodeExpression
        get() {
            val input = this.inputs.getOrNull(0)
            if (input is PhysicalNodeExpression) {
                return input
            } else {
                throw Exception()
            }
        }
}