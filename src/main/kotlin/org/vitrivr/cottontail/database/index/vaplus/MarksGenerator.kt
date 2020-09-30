package org.vitrivr.cottontail.database.index.vaplus

import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow

object MarksGenerator {
    /** */
    const val EPSILON = 10E-9

    /**
     * Get marks.
     */
    fun getNonUniformMarks(data: Array<DoubleArray>, marksPerDimension: IntArray): Array<DoubleArray> {
        val marks = getEquidistantMarks(data, marksPerDimension).map {
            val copy = it.copyOf(it.size + 1)
            copy[copy.size - 1] = copy[copy.size - 2] + EPSILON
            copy
        }.toTypedArray()

        /**
         * Iterate over dimensions.
         * Get marks of d-th dimension.
         * Do k-means.
         */
        marksPerDimension.indices.map { d ->
            var delta: Double
            var deltaBar = Double.POSITIVE_INFINITY
            do {
                // k-means
                delta = deltaBar

                // pseudocode line 3
                /**
                 * Iterate over marks of d-th dimension.
                 * Iterate over data.
                 * Check if data point is in interval of marks.
                 * Calculate mean of data points in interval (rj).
                 * Return list of mean of every interval in d (rjs).
                 */
                val rjs = Array(marks[d].size - 1) { c ->
                    var rj = 0.0
                    var rjAll = 0.0
                    var count = 0
                    var countAll = 0
                    data.forEach {
                        if (it[d] >= marks[d][c] && it[d] < marks[d][c + 1]) {
                            rj += it[d]
                            count += 1
                        }
                        rjAll += it[d]
                        countAll += 1
                    }
                    if (count == 0) {
                        rjAll / countAll
                    } else {
                        rj / count
                    }
                }

                // pseudocode line 7
                /**
                 * Iterate over marks of d-th dimension.
                 * Adjust marks (moving along distance, no long equidistance)
                 * The mark at position c is adjusted with "(first mean value + second mean value) / 2"
                 */
                (1 until marks[d].size - 1).forEach { c ->
                    marks[d][c] = (rjs[c - 1] + rjs[c]) / 2
                }

                // pseudocode line 8
                /**
                 * Iterate over marks of d-th dimension.
                 * Iterate over data.
                 * Check if data point is in interval of (new) marks.
                 * If so, apply formula:
                 * tmp = (difference between data point and c).pow(2) = euclidean distance
                 * if distance > 0.999 then break
                 */
                deltaBar = (0 until marks[d].size - 1).sumByDouble { c ->
                    var tmp = 0.0
                    data.forEach {
                        if (it[d] >= marks[d][c] && it[d] < marks[d][c + 1]) {
                            tmp += (it[d] - rjs[c]).pow(2)
                        }
                    }
                    tmp
                }
            } while ((delta - deltaBar) / delta < 0.999)
        }
        return marks
    }

    /**
     * Create marks per dimension (equally spaced).
     */
    fun getEquidistantMarks(data: Array<DoubleArray>, marksPerDimension: IntArray): Array<DoubleArray> {
        val min = getMin(data)
        val max = getMax(data)
        return Array(min.size) { i ->
            DoubleArray(marksPerDimension[i] * 2) {
                it * (max[i] - min[i]) / (marksPerDimension[i] * 2 - 1) + min[i]
            }
        }
    }

    /**
     * Get vector ([DoubleArray]) which values are minimal.
     */
    private fun getMin(data: Array<DoubleArray>): DoubleArray {
        val out = DoubleArray(data.first().size) { Double.MAX_VALUE }
        for (array in data) {
            for (i in array.indices) {
                out[i] = min(out[i], array[i])
            }
        }
        return out
    }

    /**
     * Get vector ([DoubleArray]) which values are maximal.
     */
    private fun getMax(data: Array<DoubleArray>): DoubleArray {
        val out = DoubleArray(data.first().size) { Double.MIN_VALUE }
        for (array in data) {
            for (i in array.indices) {
                out[i] = max(out[i], array[i])
            }
        }
        return out
    }

}
