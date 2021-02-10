package org.vitrivr.cottontail.database.index.va.bounds

import org.vitrivr.cottontail.database.index.va.signature.Marks
import org.vitrivr.cottontail.database.index.va.signature.VAFSignature
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import kotlin.math.max
import kotlin.math.pow

/**
 * A [Bounds] implementation for squared L2 (Euclidean) distance. Based on [1].
 *
 * References:
 * [1] Weber, R. and Blott, S., 1997. An approximation based data structure for similarity search (No. 9141, p. 416). Technical Report 24, ESPRIT Project HERMES.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class L2SBounds(query: RealVectorValue<*>, marks: Marks) : Bounds {

    /** Lower bound of this [L2Bounds]. */
    override var lb = 0.0
        private set

    /** Upper bound of this [L2Bounds]. */
    override var ub = 0.0
        private set

    /** Cells for the query [RealVectorValue]. */
    private val rq = marks.getCells(query)

    /** Internal lookup table for pre-calculated values used in bounds calculation. */
    private val lat = Array(marks.marks.size) { j ->
        val qj = query[j].value.toDouble()
        Array(marks.marks[j].size) { m ->
            /*
             * Simplification and deviation from [1], because for L2 there is only one value here.
             *
             * (qj - marks.marks[j][m]).pow(2) == abs(marks.marks[j][m] - qj).pow(2)
             */
            (qj - marks.marks[j][m]).pow(2)
        }
    }

    /**
     * Updates the lower and upper bounds of this [L2SBounds] for the given [VAFSignature].
     *
     * @param signature The [VAFSignature] to calculate the bounds for.
     */
    override fun update(signature: VAFSignature): L2SBounds {
        this.lb = 0.0
        this.ub = 0.0
        for ((j, rij) in signature.cells.withIndex()) {
            when {
                rij < this.rq[j] -> {
                    this.lb += this.lat[j][rij + 1]
                    this.ub += this.lat[j][rij]
                }
                rij == this.rq[j] -> {
                    this.ub += max(this.lat[j][rij], this.lat[j][rij + 1])
                }
                rij > this.rq[j] -> {
                    this.lb += this.lat[j][rij]
                    this.ub += this.lat[j][rij + 1]
                }
            }
        }
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
        var lb = 0.0
        for ((j, rij) in signature.cells.withIndex()) {
            if (rij < this.rq[j]) {
                lb += this.lat[j][rij + 1]
            } else if (rij > this.rq[j]) {
                lb += this.lat[j][rij]
            }
            if (lb >= threshold) {
                return false
            }
        }
        return lb < threshold
    }
}