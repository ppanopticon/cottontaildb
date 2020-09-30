package org.vitrivr.cottontail.database.index.vaplus

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

//data class VAPlusSignature(val tupleId: Long, val signature: BitSet) {}
data class VAPlusSignature(val tupleId: Long, val signature: IntArray)

object VAPlusSignatureSerializer : Serializer<VAPlusSignature> {

    override fun serialize(out: DataOutput2, value: VAPlusSignature) {
        //val signatureArray = value.signature.toLongArray()
        val signatureArray = value.signature
        out.writeLong(value.tupleId)
        out.packInt(signatureArray.size)
        //out.packLongArray(signatureArray, 0, signatureArray.size)
        for (signature in signatureArray) {
            out.writeInt(signature)
        }
    }

    override fun deserialize(input: DataInput2, available: Int): VAPlusSignature {
        val tupleId = input.readLong()
        //val signature = LongArray(input.unpackInt())
        val signature = IntArray(input.unpackInt()) {
            input.readInt()
        }
        //input.unpackLongArray(signature, 0, signature.size)
        //return VAPlusSignature(tupleId, BitSet.valueOf(signature))
        return VAPlusSignature(tupleId, signature)
    }

}
