package org.vitrivr.cottontail.utilities.extensions

import java.util.*

/**
 * Initializes this [BitSet] using the provided initialization function.
 *
 * @param init Function used to initialize this [BitSet].
 * @return The initialized [BitSet].
 */
fun BitSet.init(init: (Int) -> Boolean): BitSet {
    for (i in 0 until this.size()) {
        this.set(i, init(i))
    }
    return this
}

/**
 * Returns a new boolean array containing all the bits in this bit set.
 *
 * @return [BooleanArray] representation of this [BitSet].
 */
fun BitSet.toBooleanArray(): BooleanArray = BooleanArray(this.size()) { this[it] }