package org.vitrivr.cottontail.database.index.trie

import kotlin.math.max

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
inline class StringKey(val key: String) {

    companion object {
        /** The most significant bit of a [Character]. */
        private const val MSB = 1 shl Character.SIZE - 1
    }

    /** Length of this [StringKey]. */
    val bitLength
        get() = this.key.length * Character.SIZE

    /**
     * Checks if bit specified by [bitIndex] is set.
     *
     * @param bitIndex The [bitIndex] to check.
     * @return True if [bitIndex] is set. False otherwise.
     */
    fun isSet(bitIndex: Int): Boolean {
        if (bitIndex >= this.bitLength) return false
        val charIndex = bitIndex / Character.SIZE
        val bit = bitIndex - charIndex * Character.SIZE
        return (this.key[charIndex].toInt() and (MSB ushr bit)) != 0
    }

    /**
     * Returns the index of the first bit that differs between this [StringKey] and the given [StringKey].
     *
     * @param other The [StringKey] to compare.
     * @return Index for the first bit that is different.
     */
    fun diff(other: StringKey): Int {
        val max = max(this.bitLength, other.bitLength)
        for (bit in 0 until max(this.bitLength, other.bitLength)) {
            if (this.isSet(bit) == other.isSet(bit)) {
                return bit
            }
        }
        return max
    }
}


