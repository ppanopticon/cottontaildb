package org.vitrivr.cottontail.database.index.va.signature

import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow

object MarksGenerator {
    /** */
    const val EPSILON = 1E-9

    /**
     * Get marks.
     * Todo: Tests indicate that these marks are less tight than the equidistant ones
     */
    fun getNonUniformMarks(data: Array<DoubleArray>, marksPerDimension: IntArray): Marks {
        val marks = getEquidistantMarks(data, marksPerDimension)

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
                val rjs = Array(marks.marks[d].size - 1) { c ->
                    var rj = 0.0
                    var rjAll = 0.0
                    var count = 0
                    var countAll = 0
                    data.forEach {
                        if (it[d] >= marks.marks[d][c] && it[d] < marks.marks[d][c + 1]) {
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
                (1 until marks.marks[d].size - 1).forEach { c ->
                    marks.marks[d][c] = (rjs[c - 1] + rjs[c]) / 2
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
                deltaBar = (0 until marks.marks[d].size - 1).sumByDouble { c ->
                    var tmp = 0.0
                    data.forEach {
                        if (it[d] >= marks.marks[d][c] && it[d] < marks.marks[d][c + 1]) {
                            tmp += (it[d] - rjs[c]).pow(2)
                        }
                    }
                    tmp
                }
            } while ((delta - deltaBar) / delta < 0.999)
            marks.marks[d].sort()
        }
        return marks
    }

    /**
     * Create marks per dimension (equally spaced). Min and Max of data are included -> only makes sense to require
     * at least 3 marks
     * note: blott & weber are referring to marks that yield equally populated regions, not equidistant marks
     */
    fun getEquidistantMarks(data: Array<DoubleArray>, marksPerDimension: IntArray): Marks {
        val min = getMin(data)
        val max = getMax(data)
        return getEquidistantMarks(min, max, marksPerDimension)
    }

    fun getEquidistantMarks(
        min: DoubleArray,
        max: DoubleArray,
        marksPerDimension: IntArray
    ): Marks {
        return Marks(Array(min.size) { i ->
            require(marksPerDimension[i] > 2) { "Need to request more than 2 mark per dimension! (Faulty dimension: $i)" }
            val a = DoubleArray(marksPerDimension[i]) {
                min[i] + it * (max[i] - min[i]) / (marksPerDimension[i] - 1)
            }// subtract small amount to ensure min is included to avoid problems with FP approximations
            // also add small amount for last
            a[0] -= EPSILON
            a[a.lastIndex] += EPSILON
            a
        })
    }

    /**
     * Create marks per dimension (equally spaced). Min and max of data are not included! -> 1 mark is in middle
     * of data range, 2 marks divide the range into 3 thirds, etc...
     */
    fun getEquidistantMarksWithoutMinMax(
        data: Array<DoubleArray>,
        marksPerDimension: IntArray
    ): Marks {
        val min = getMin(data)
        val max = getMax(data)
        return Marks(Array(min.size) { i ->
            require(marksPerDimension[i] > 0) { "Need to request at least 1 mark per dimension! (Faulty dimension: $i)" }
            val range = max[i] - min[i]
            val spacing = range / (marksPerDimension[i] + 1)
            DoubleArray(marksPerDimension[i]) {
                min[i] + (it + 1) * spacing
            }
        })
    }

    /**
     * pseudocode: we have k = N / (marksPerDimension[d] - 1) elements per region for dimension d
     * easiest is to just sort each dimension and take kth and k+1th value and put mark in between
     * quickSelect would probably have better performance, but needs custom implementation
     *
     */
    fun getEquallyPopulatedMarks(data: Array<DoubleArray>, marksPerDimension: IntArray): Marks {
        // can we do a transpose of the data so that we have an array of components for each dimension that
        // we can sort? Easiest is probably to copy, but this isn't gonna be cheap on ram...
        return Marks(Array(marksPerDimension.size) { dim ->
            val n = marksPerDimension[dim]
            val vecsPerRegion =
                (data.size / (n - 1)) // check effects of constant rounding down... probably last region gets more on avg
            require(vecsPerRegion > 0) { "More regions than data! Better use equidistant marks!" }
            val dimData = DoubleArray(data.size) { data[it][dim] }
            dimData.sort()
            val firstMark = dimData.first() - EPSILON
            val lastMark = dimData.last() + EPSILON
            DoubleArray(n) { m ->
                when (m) {
                    0 -> firstMark
                    marksPerDimension[dim] - 1 -> lastMark
                    else -> {
                        dimData[m * vecsPerRegion] + (dimData[m * vecsPerRegion + 1] - dimData[m * vecsPerRegion]) / 2
                    }
                }
            }
        })
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
