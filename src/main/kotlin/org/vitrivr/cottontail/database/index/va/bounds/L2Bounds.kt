package org.vitrivr.cottontail.database.index.va.bounds

import org.vitrivr.cottontail.database.index.va.signature.Marks
import org.vitrivr.cottontail.database.index.va.signature.Signature
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import kotlin.math.max
import kotlin.math.pow
import kotlin.math.sqrt

/**
 * A [Bounds] implementation for L2 (Euclidean) distance. This is equivalent to the implementation
 * in [LpBounds] but more efficient. Based on [1].
 *
 * References:
 * [1] Weber, R. and Blott, S., 1997. An approximation based data structure for similarity search (No. 9141, p. 416). Technical Report 24, ESPRIT Project HERMES.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class L2Bounds(query: RealVectorValue<*>, marks: Marks) : Bounds {

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
            doubleArrayOf((qj - marks.marks[j][m]).pow(2), (marks.marks[j][m] - qj).pow(2))
        }
    }

    /**
     * Updates the lower and upper bounds of this [L2Bounds] for the given [Signature].
     *
     * @param signature The [Signature] to calculate the bounds for.
     */
    override fun update(signature: Signature): L2Bounds {
        var lb = 0.0
        var ub = 0.0
        val ri = signature.cells
        for (j in signature.cells.indices) {
            when {
                ri[j] < this.rq[j] -> {
                    lb += this.lat[j][ri[j] + 1][0]
                    ub += this.lat[j][ri[j]][0]
                }
                ri[j] == this.rq[j] -> {
                    ub += max(this.lat[j][ri[j]][0], this.lat[j][ri[j] + 1][1])
                }
                ri[j] > this.rq[j] -> {
                    lb += this.lat[j][ri[j]][1]
                    ub += this.lat[j][ri[j] + 1][1]
                }
            }
        }
        this.lb = sqrt(lb)
        this.ub = sqrt(ub)
        return this
    }
}