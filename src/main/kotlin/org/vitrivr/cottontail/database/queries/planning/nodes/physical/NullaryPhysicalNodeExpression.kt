package org.vitrivr.cottontail.database.queries.planning.nodes.physical

/**
 * An abstract [PhysicalNodeExpression] implementation that has no input node.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class NullaryPhysicalNodeExpression : PhysicalNodeExpression() {
    /** The arity of the [NullaryPhysicalNodeExpression] is always on. */
    override val inputArity = 0

    /** True, if this [NullaryPhysicalNodeExpression] can be partitioned, false otherwise. */
    abstract val canBePartitioned: Boolean

    /**
     * Tries to create [p] partitions of this [NullaryPhysicalNodeExpression] if possible. If the implementing
     * [NullaryPhysicalNodeExpression] returns true for [canBePartitioned], then this method is expected
     * to return a result, even if the number of partitions returned my be lower than [p] or even one (which
     * means that no partitioning took place).
     *
     * If [canBePartitioned] returns false, this method is expected to throw a [IllegalStateException].
     *
     * @param p The desired number of partitions.
     * @return Array of [NullaryPhysicalNodeExpression]s.
     *
     * @throws IllegalStateException If this [NullaryPhysicalNodeExpression] cannot be partitioned.
     */
    abstract fun partition(p: Int): List<NullaryPhysicalNodeExpression>
}