package org.vitrivr.cottontail.utilities.string

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object BitwiseStringUtilities {

    /** The most significant bit of a [Character]. */
    private const val MSB = 1 shl Character.SIZE - 1

    /**
     * Compares two [String]s and returns the position of the first bit at which they differ.
     *
     * @param v1 First [String] for the comparison.
     * @param v2 Second [String] for the comparison.
     * @return First bit at which two [String]s differ.
     */
    fun compare(v1: String?, v2: String?): Int {
        var bit = 0
        while (isSet(v1, bit) == isSet(v2, bit)) {
            ++bit
            if (bit > (v1?.length ?: 0) || bit > (v2?.length ?: 0)) {
                break
            }
        }
        return bit
    }

    /**
     * Checks if the i-th bit in the given [String] is set.
     *
     * @param v [String] to check.
     * @param i: Index of bit to check.
     * @return True if i-th bit is set, false otherwise.
     */
    fun isSet(v: String?, i: Int): Boolean {
        if (v == null) return false
        val charIndex = i / Character.SIZE
        val bitIndex = i - charIndex * Character.SIZE
        if (charIndex >= v.length) return false
        return (v[charIndex].toInt() and (MSB ushr bitIndex)) != 0
    }
}