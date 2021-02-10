package org.vitrivr.cottontail.database.index.va.bounds

import org.vitrivr.cottontail.database.index.va.signature.Marks
import org.vitrivr.cottontail.database.index.va.signature.VAFSignature
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import kotlin.math.max
import kotlin.math.pow

/**
 * A [Bounds] implementation for Lp distance. Based on [1].
 *
 * References:
 * [1] Weber, R. and Blott, S., 1997. An approximation based data structure for similarity search (No. 9141, p. 416). Technical Report 24, ESPRIT Project HERMES.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LpBounds(query: RealVectorValue<*>, marks: Marks, val p: Int) : Bounds {

    /** Lower bound of this [LpBounds]. */
    override var lb = 0.0
        private set

    /** Upper bound of this [LpBounds]. */
    override var ub = 0.0
        private set

    /** Exponent used for p-th root calculation. */
    private val exp = 1.0 / p

    /** Cells for the query [RealVectorValue]. */
    private val rq = marks.getCells(query)

    /** Internal lookup table for pre-calculated values used in bounds calculation. */
    private val lat = Array(marks.marks.size) { j ->
        val qj = query[j].value.toDouble()
        Array(marks.marks[j].size) { m ->
            doubleArrayOf((qj - marks.marks[j][m]).pow(p), (marks.marks[j][m] - qj).pow(p))
        }
    }

    /**
     * Updates the lower and upper bounds of this [L2Bounds] for the given [VAFSignature].
     *
     * @param signature The [VAFSignature] to calculate the bounds for.
     */
    override fun update(signature: VAFSignature): LpBounds {
        this.lb = 0.0
        this.ub = 0.0
        val ri = signature.cells
        for (j in signature.cells.indices) {
            when {
                ri[j] < this.rq[j] -> {
                    this.lb += this.lat[j][ri[j] + 1][0]
                    this.ub += this.lat[j][ri[j]][0]
                }
                ri[j] == this.rq[j] -> {
                    this.ub += max(this.lat[j][ri[j]][0], this.lat[j][ri[j] + 1][1])
                }
                ri[j] > this.rq[j] -> {
                    this.lb += this.lat[j][ri[j]][1]
                    this.ub += this.lat[j][ri[j] + 1][1]
                }
            }
        }
        this.lb = this.lb.pow(this.exp)
        this.ub = this.ub.pow(this.exp)
        return this
    }

    /**
     * Checks if the given [VAFSignature] is a VA-SSA candidate according to [1] by comparing the
     * lower bounds estimation to the given threshold and returns true if so and false otherwise.
     *
     * @param signature The [VAFSignature] to check.
     * @param threshold The threshold for a [VAFSignature] to be deemed a candidate. Can be used for early stopping.
     * @return True if [VAFSignature] is a candidate, false otherwise.
     */
    override fun isVASSACandidate(signature: VAFSignature, threshold: Double): Boolean {
        val tsquared = threshold.pow(this.p)
        var lb = 0.0
        for ((j, rij) in signature.cells.withIndex()) {
            if (rij < this.rq[j]) {
                lb += this.lat[j][rij + 1][0]
            } else if (rij > this.rq[j]) {
                lb += this.lat[j][rij][1]
            }
            if (lb >= tsquared) {
                return false
            }
        }
        return lb < tsquared
    }
}