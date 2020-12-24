package org.vitrivr.cottontail.database.queries.planning.cost

import java.util.*
import kotlin.math.max
import kotlin.system.measureNanoTime

/**
 * Represents a unit of [Cost]. Used to measure and compare operations in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
data class Cost constructor(val io: Float = 0.0f, val cpu: Float = 0.0f, val memory: Float = 0.0f) : Comparable<Cost> {

    companion object {
        val ZERO = Cost(0.0f, 0.0f, 0.0f)
        val INVALID = Cost(Float.NaN, Float.NaN, Float.NaN)

        /** Number of repetitions to run when estimating costs. */
        private const val ESTIMATION_REPETITION = 1_000_000

        /** Constant used to estimate, how much parallelization makes sense given CPU [Cost]s. This is a magic number :-) */
        private const val PARALLELISATION_CONSTANT = 10.0f

        /** Cost read access to disk. TODO: Estimate based on local hardware. */
        const val COST_DISK_ACCESS_READ = 1e-4f

        /** Cost read access to disk. TODO: Estimate based on local hardware. */
        const val COST_DISK_ACCESS_WRITE = 5 * 1e-4f

        /** Estimated cost of memory access. */
        val COST_MEMORY_ACCESS = estimateMemoryAccessCost()

        /** Estimated cost of a floating point operation. */
        val COST_FLOP = estimateFlopCost()

        /* Default selectivity for boolean predicates. */
        const val COST_DEFAULT_SELECTIVITY = 0.5f

        /**
         * Estimates the cost of memory access  based on a series of measurements.
         */
        private fun estimateMemoryAccessCost(): Float {
            val random = SplittableRandom()
            var time = 0L
            repeat(ESTIMATION_REPETITION) {
                var a = random.nextLong()
                var b = random.nextLong()
                var c = 0L
                time += measureNanoTime {
                    c = a
                    a = b
                    b = c
                }
                c + b
            }
            return ((time) / (ESTIMATION_REPETITION * 3)) * 1e-9f
        }

        /**
         * Estimates the cost of a single floating point operation based on a series of measurements
         * for add, subtraction, multiplication and division
         */
        private fun estimateFlopCost(): Float {
            val random = SplittableRandom()
            var timeAdd = 0L
            repeat(ESTIMATION_REPETITION) {
                val a = random.nextDouble()
                val b = random.nextDouble()
                timeAdd += measureNanoTime {
                    a + b
                }
            }

            var timeSubtract = 0L
            repeat(ESTIMATION_REPETITION) {
                val a = random.nextDouble()
                val b = random.nextDouble()
                timeSubtract += measureNanoTime {
                    a - b
                }
            }

            var timeMultiply = 0L
            repeat(ESTIMATION_REPETITION) {
                val a = random.nextDouble()
                val b = random.nextDouble()
                timeMultiply += measureNanoTime {
                    a * b
                }
            }

            var timeDivide = 0L
            repeat(ESTIMATION_REPETITION) {
                val a = random.nextDouble()
                val b = random.nextDouble(1.0)
                timeDivide += measureNanoTime {
                    a / b
                }
            }

            return ((timeAdd + timeSubtract + timeMultiply + timeDivide) / (ESTIMATION_REPETITION * 4)) * 1e-9f
        }
    }

    /**
     * Estimates, how much parallelisation makes sense given this [Cost]s CPU cost.
     *
     * @return Parallelisation estimation for this [Cost].
     */
    fun parallelisation() = max(((this.cpu - this.io) / PARALLELISATION_CONSTANT).toInt(), 1)


    operator fun plus(other: Cost): Cost = Cost(this.io + other.io, this.cpu + other.cpu, this.memory + other.memory)
    operator fun minus(other: Cost): Cost = Cost(this.io - other.io, this.cpu - other.cpu, this.memory - other.memory)
    operator fun times(other: Cost): Cost = Cost(this.io * other.io, this.cpu * other.cpu, this.memory * other.memory)
    operator fun div(other: Cost): Cost = Cost(this.io / other.io, this.cpu / other.cpu, this.memory / other.memory)
    operator fun plus(other: Number): Cost = Cost(this.io + other.toFloat(), this.cpu + other.toFloat(), this.memory + other.toFloat())
    operator fun minus(other: Number): Cost = Cost(this.io - other.toFloat(), this.cpu - other.toFloat(), this.memory - other.toFloat())
    operator fun times(other: Number): Cost = Cost(this.io * other.toFloat(), this.cpu * other.toFloat(), this.memory * other.toFloat())
    operator fun div(other: Number): Cost = Cost(this.io / other.toFloat(), this.cpu / other.toFloat(), this.memory / other.toFloat())
    override fun compareTo(other: Cost): Int = (2.0f * (this.cpu - other.cpu) + 1.25f * (this.io - other.io) + (this.memory - other.memory)).toInt()
}