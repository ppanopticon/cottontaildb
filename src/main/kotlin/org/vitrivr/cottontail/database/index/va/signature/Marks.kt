package org.vitrivr.cottontail.database.index.va.signature

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.RealVectorValue

/**
 * Double precision [Marks] implementation used in [VAFIndex] structures.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.0.0
 */
inline class Marks(val marks: Array<DoubleArray>) {

    companion object Serializer : org.mapdb.Serializer<Marks> {
        override fun serialize(out: DataOutput2, value: Marks) {
            out.packInt(value.marks.size)
            value.marks.forEach { dim ->
                org.mapdb.Serializer.DOUBLE_ARRAY.serialize(out, dim)
            }
        }

        override fun deserialize(input: DataInput2, available: Int) =
            Marks(Array(input.unpackInt()) {
                org.mapdb.Serializer.DOUBLE_ARRAY.deserialize(input, available)
            })
    }

    /**
     * This methods calculates the signature of a [RealVectorValue]. It checks for every mark if the
     * corresponding vector is beyond that mark or not. If so, the preceding mark is the corresponding mark.
     *
     * Note that this method can return -1, which means that the component is smaller than the smallest mark!
     * This can arise, e.g., if marks are not generated from entire dataset, but just a sampled subset thereof!
     *
     * @param vector The [RealVectorValue] to calculate the cells for.
     * @return An [IntArray] containing the signature of the vector.
     */
    fun getCells(vector: RealVectorValue<*>): IntArray = IntArray(vector.logicalSize) {
        val index = this.marks[it].indexOfFirst { i -> i > vector[it].value.toDouble() }
        if (index == -1) { // all marks are less or equal than the vector component! last mark is what we're looking for!
            this.marks[it].size - 1
        } else {
            index - 1
        }
    }
}