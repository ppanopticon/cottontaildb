package org.vitrivr.cottontail.database.index.vaplus

import org.apache.commons.math3.linear.MatrixUtils
import org.apache.commons.math3.linear.RealMatrix
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

data class VAPlusMeta(val marks: Array<DoubleArray>, val signatureGenerator: SignatureGenerator, val kltMatrix: RealMatrix)

object VAPlusMetaSerializer : Serializer<VAPlusMeta> {

    override fun serialize(out: DataOutput2, value: VAPlusMeta) {
        out.packInt(value.marks.size)
        for (mark in value.marks) {
            out.packInt(mark.size)
            for (m in mark) {
                out.writeDouble(m)
            }
        }
        out.packInt(value.signatureGenerator.numberOfBitsPerDimension.size)
        for (bits in value.signatureGenerator.numberOfBitsPerDimension) {
            out.writeInt(bits)
        }
        out.packInt(value.kltMatrix.data.size)
        for (kltM in value.kltMatrix.data) {
            out.packInt(kltM.size)
            for (klt in kltM) {
                out.writeDouble(klt)
            }
        }
    }

    override fun deserialize(input: DataInput2, available: Int): VAPlusMeta {
        val marks = Array(input.unpackInt()) {
            DoubleArray(input.unpackInt()) {
                input.readDouble()
            }
        }
        val signatureGenerator = IntArray(input.unpackInt()) {
            input.readInt()
        }
        val kltMatrix = Array(input.unpackInt()) {
            DoubleArray(input.unpackInt()) {
                input.readDouble()
            }
        }
        return VAPlusMeta(marks, SignatureGenerator(signatureGenerator), MatrixUtils.createRealMatrix(kltMatrix))
    }

}
