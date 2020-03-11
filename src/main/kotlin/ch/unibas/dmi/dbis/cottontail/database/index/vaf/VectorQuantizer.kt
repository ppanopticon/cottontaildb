package ch.unibas.dmi.dbis.cottontail.database.index.vaf

import java.lang.IndexOutOfBoundsException
import java.util.*

class VectorQuantizer {

    private val scalarQuantizers: List<ScalarQuantizer>
    private val bits: Int

    internal constructor(scalarQuantizers: List<ScalarQuantizer>) {
        this.scalarQuantizers = scalarQuantizers
        bits = scalarQuantizers.sumBy { it.bits.toInt() }
    }

    constructor(vectors: List<FloatArray>, bits: UByte) {
        val length = vectors.first().size

        scalarQuantizers = (0 until length).map { idx ->
            val values = mutableListOf<Float>()
            vectors.forEach {
                values.add(it[idx])
            }
            ScalarQuantizer(values.toFloatArray(), bits)
        }

        this.bits = bits.toInt() * length
    }

    fun quantize(vector: FloatArray): BitSet {
        if (vector.size != scalarQuantizers.size) {
            throw IndexOutOfBoundsException("vector length ${vector.size} does not match index size ${scalarQuantizers.size}")
        }

        val bitset = BitSet(this.bits)

        var offset: Int = 0

        scalarQuantizers.forEachIndexed { index, scalarQuantizer ->
            val q = scalarQuantizer.quantize(vector[index])
            q.forEach {
                if (it) {
                    bitset.set(offset++)
                } else {
                    bitset.clear(offset++)
                }
            }
        }
        return bitset
    }

}