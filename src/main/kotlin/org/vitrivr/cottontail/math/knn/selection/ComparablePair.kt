package org.vitrivr.cottontail.math.knn.selection

data class ComparablePair<A, B : Comparable<B>>(val first: A, val second: B) : Comparable<ComparablePair<A, B>> {
    override fun compareTo(other: ComparablePair<A, B>): Int = this.second.compareTo(other.second)
}