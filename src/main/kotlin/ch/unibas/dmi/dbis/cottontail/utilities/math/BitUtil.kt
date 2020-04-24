package ch.unibas.dmi.dbis.cottontail.utilities.math

/**
 * Utility class for bit mathematics.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object BitUtil {
    /**
     * Returns the next value that is a power of two and that is greater or equal than the given value.
     *
     * @param value Value to calculate the next power of two for.
     * @return Calculated value
     */
    fun nextPowerOfTwo(value: Long): Long {
        var result = value - 1
        result = result or (result shr 256)
        result = result or (result shr 128)
        result = result or (result shr 64)
        result = result or (result shr 32)
        result = result or (result shr 16)
        result = result or (result shr 8)
        result = result or (result shr 4)
        result = result or (result shr 2)
        result = result or (result shr 1)
        return result + 1
    }

    /**
     * Returns the next value that is a power of two and that is greater or equal than the given value.
     *
     * @param value Value to calculate the next power of two for.
     * @return Calculated value
     */
    fun nextPowerOfTwo(value: Int): Int {
        var result = value - 1
        result = result or (result shr 16)
        result = result or (result shr 8)
        result = result or (result shr 4)
        result = result or (result shr 2)
        result = result or (result shr 1)
        return result + 1
    }

    fun toShift(value: Int) = Integer.numberOfTrailingZeros(value)
}