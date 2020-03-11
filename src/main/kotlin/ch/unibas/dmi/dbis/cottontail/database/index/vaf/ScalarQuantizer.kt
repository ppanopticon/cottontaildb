package ch.unibas.dmi.dbis.cottontail.database.index.vaf

import kotlin.math.log2

class ScalarQuantizer {

    private val bounds: FloatArray
    private val bitLen: Int
    val bits: UByte

    constructor(vector: FloatArray, bits: UByte) {
        bitLen = 1 shl bits.toInt()
        bounds = quantizeVector(vector)
        this.bits = bits
    }

    internal constructor(bounds: FloatArray) {
        this.bounds = bounds
        this.bitLen = bounds.size + 1
        this.bits = log2(bitLen.toDouble()).toUInt().toUByte()
    }

    private fun quantizeVector(vector: FloatArray): FloatArray {
        val values = vector.filter { it.isFinite() }.sorted()
        val indexes = (1 until bitLen).map { (it * values.size) / bitLen }
        return indexes.map { values[it] }.toFloatArray()
    }

    fun quantize(value: Float): BooleanArray {
        val index = bounds.binarySearch(value)
        val value = if (index >= 0) index else -index - 1
        return (bits.toInt() - 1 downTo 0).map {
            (value and (1 shl it)) != 0
        }.toBooleanArray()
    }

    fun getRange(index: Int): ClosedRange<Float>? {
        return when (index) {
            0 -> Float.NEGATIVE_INFINITY.rangeTo(bounds.first())
            in 1 until bitLen - 1 -> bounds[index - 1].rangeTo(bounds[index])
            bitLen - 1 -> bounds.last().rangeTo(Float.POSITIVE_INFINITY)
            else -> null
        }
    }

}