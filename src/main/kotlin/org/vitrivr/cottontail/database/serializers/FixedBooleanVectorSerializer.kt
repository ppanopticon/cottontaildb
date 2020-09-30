package org.vitrivr.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.BooleanVectorValue
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.serializer.Serializer
import kotlin.math.min

/**
 * A [Serializer] for [BooleanVectorValue]s that a fixed in length.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FixedBooleanVectorSerializer(val size: Int): Serializer<BooleanVectorValue> {

    /** Size of the array used to store the values. */
    private val arraySize = bitToWordIndex(this.size - 1) + 1

    companion object {

        /** */
        private const val LONG_BIT_SHIFT = 6

        /**
         * Given a bit index, return word index containing it.
         *
         * @param bitIndex The bit index to calculate the word index for.
         */
        private fun bitToWordIndex(bitIndex: Int): Int = bitIndex shr LONG_BIT_SHIFT
    }

    override fun serialize(out: DataOutput2, value: BooleanVectorValue) {
        val words = LongArray(this.arraySize) {
            var v = 0L
            for (idx in (it shl LONG_BIT_SHIFT) until min((it + 1) shl LONG_BIT_SHIFT, this.size)) {
                if (value.data[idx]) {
                    v = v or (1L shl idx)
                }
            }
            v
        }
        out.packLongArray(words, 0, this.arraySize)
    }

    override fun deserialize(input: DataInput2, available: Int): BooleanVectorValue {
        val words = LongArray(this.arraySize)
        input.unpackLongArray(words, 0, this.arraySize)
        return BooleanVectorValue(BooleanArray(this.size) {
            val long = words[bitToWordIndex(it)]
            val check = (1L shl it)
            (long and check) == check
        })
    }

    override val logicalSize: Int
        get() = TODO("Not yet implemented")
    override val physicalSize: Int
        get() = TODO("Not yet implemented")

    override fun serialize(page: Page, offset: Int, value: BooleanVectorValue) {
        TODO("Not yet implemented")
    }

    override fun deserialize(page: Page, offset: Int): BooleanVectorValue {
        TODO("Not yet implemented")
    }
}