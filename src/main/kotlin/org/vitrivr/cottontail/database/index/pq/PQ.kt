package org.vitrivr.cottontail.database.index.pq

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.column.Type
import org.vitrivr.cottontail.database.index.pq.codebook.DoublePrecisionPQCodebook
import org.vitrivr.cottontail.database.index.pq.codebook.PQCodebook
import org.vitrivr.cottontail.database.index.pq.codebook.SinglePrecisionPQCodebook
import org.vitrivr.cottontail.math.knn.metrics.DistanceKernel
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.model.values.FloatVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import java.util.*
import kotlin.collections.ArrayList

/**
 * Product Quantizer (PQ) that minimizes inner product error. Input data should be permuted for better results!
 *
 * Roughly following Guo et al. 2015 - Quantization based Fast Inner Product Search
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.0.0
 */
class PQ(val type: Type<*>, val codebooks: List<PQCodebook<VectorValue<*>>>) {

    /** The number of subspaces as defined in this [PQ] implementation. */
    val numberOfSubspaces
        get() = this.codebooks.size

    /** The number of dimensions per subspace which is simply the dimensionality of the [ColumnDef] divided by the number of subspaces. */
    val dimensionsPerSubspace
        get() = this.type.logicalSize / this.codebooks.size

    /**
     * Serializer for [PQ]
     */
    object Serializer : org.mapdb.Serializer<PQ> {
        /**
         * Serializes the content of the given value into the given
         * [DataOutput2].
         * todo: figure out why cleaner way via value.type.serializer(size) does not work... it's a generics issue
         *       wrt. in/out
         *
         * @param out DataOutput2 to save object into
         * @param value Object to serialize
         */
        override fun serialize(out: DataOutput2, value: PQ) {
            out.writeUTF(value.type.name)
            out.packInt(value.type.logicalSize)
            out.packInt(value.codebooks.size)
            value.codebooks.forEach {
                when (val cast = it as PQCodebook<*>) {
                    is DoublePrecisionPQCodebook -> DoublePrecisionPQCodebook.Serializer.serialize(
                        out,
                        cast
                    )
                    is SinglePrecisionPQCodebook -> SinglePrecisionPQCodebook.Serializer.serialize(
                        out,
                        cast
                    )
                }
            }
        }

        /**
         * Deserializes and returns the content of the given [DataInput2].
         *
         * @param input DataInput2 to de-serialize data from
         * @param available how many bytes that are available in the DataInput2 for
         * reading, may be -1 (in streams) or 0 (null).
         *
         * @return the de-serialized content of the given [DataInput2]
         */
        override fun deserialize(input: DataInput2, available: Int): PQ {
            val type = Type.forName(input.readUTF(), input.unpackInt())
            val size = input.unpackInt()
            val codebooks = ArrayList<PQCodebook<VectorValue<*>>>(size)
            for (i in 0 until size) {
                codebooks.add(
                    when (type) {
                        is Type.Float -> SinglePrecisionPQCodebook.Serializer.deserialize(
                            input,
                            available
                        )
                        is Type.Double -> DoublePrecisionPQCodebook.Serializer.deserialize(
                            input,
                            available
                        )
                        is Type.Complex32Vector -> SinglePrecisionPQCodebook.Serializer.deserialize(
                            input,
                            available
                        )
                        is Type.Complex64Vector -> DoublePrecisionPQCodebook.Serializer.deserialize(
                            input,
                            available
                        )
                        else -> throw IllegalStateException("")
                    } as PQCodebook<VectorValue<*>>
                )
            }
            return PQ(type, codebooks)
        }
    }

    /**
     *
     */
    companion object {
        /** [Logger] instance used for logging.. */
        private val LOGGER: Logger = LoggerFactory.getLogger(PQ::class.java)

        /** Maximum number of iterations to use for k means clustering. */
        private const val MAX_ITERATIONS = 250

        /**
         * Generates a new [PQ] instance for the given [PQIndexConfig]
         */
        @Suppress("UNCHECKED_CAST")
        fun fromData(config: PQIndexConfig, column: ColumnDef<*>, data: List<VectorValue<*>>): PQ {
            LOGGER.debug("Initializing PQ from initial data.")

            /* Sanity checks. */
            require(config.numSubspaces > 0) { "Number of subspaces must be greater than zero for PQIndex." }
            require(config.numCentroids > 0) { "Number of centroids must be greater than zero for PQIndex." }
            require(column.type.logicalSize >= config.numSubspaces) { "Logical size of column must be greater or equal to number of subspaces." }
            require(column.type.logicalSize % config.numSubspaces == 0) { "Logical size of column modulo number of subspaces must be zero." }

            /* Calculate some important metrics. */
            val dimensionsPerSubspace = column.type.logicalSize / config.numSubspaces

            /* Prepare subspace data. */
            LOGGER.debug("Creating subspace data")
            val subspaceData = (0 until config.numSubspaces).map { k ->
                data.map { v -> v.subvector(k * dimensionsPerSubspace, dimensionsPerSubspace) }
            }

            /* Start learning of centroids. */
            LOGGER.debug("Learning centroids")
            val codebooks = mutableListOf<PQCodebook<VectorValue<*>>>()
            subspaceData.parallelStream().forEach { d ->
                val codebook: PQCodebook<*> = when (column.type) {
                    is Type.Complex32Vector,
                    is Type.FloatVector -> SinglePrecisionPQCodebook.learnFromData(
                        d as List<FloatVectorValue>,
                        config.numCentroids,
                        config.seed,
                        MAX_ITERATIONS
                    )
                    is Type.Complex64Vector,
                    is Type.DoubleVector -> DoublePrecisionPQCodebook.learnFromData(
                        d as List<DoubleVectorValue>,
                        config.numCentroids,
                        config.seed,
                        MAX_ITERATIONS
                    )
                    else -> throw IllegalArgumentException("VectorValue of type ${column.type} is not supported for PQ.")
                }
                codebooks.add(codebook as PQCodebook<VectorValue<*>>)
            }

            LOGGER.debug("PQ initialization done.")
            return PQ(column.type, codebooks)
        }
    }

    /**
     * Builds and returns the [PQSignature] for the specified [VectorValue],  which is simply a
     * concatenation of the representative centroid in each subspace for the specified vector
     *
     * @param v The [VectorValue] to calculate the [PQSignature] for.
     * @return The calculated [PQSignature]
     */
    fun getSignature(v: VectorValue<*>): PQSignature =
        getSignature(v, IntArray(this.numberOfSubspaces))

    /**
     * Builds and returns the [PQSignature] for the specified [VectorValue],  which is simply a
     * concatenation of the representative centroid in each subspace for the specified vector
     *
     * @param v The [VectorValue] to calculate the [PQSignature] for.
     * @return The calculated [PQSignature]
     */
    fun getSignature(v: VectorValue<*>, array: IntArray): PQSignature {
        require(array.size == this.numberOfSubspaces) { "IntArray of signature must have the same size as the number of subspaces for this PQ (expected: ${this.numberOfSubspaces}, actual: ${array.size})." }
        Arrays.parallelSetAll(array) { k ->
            this.codebooks[k].quantizeSubspaceForVector(v, k * this.dimensionsPerSubspace)
        }
        return PQSignature(array)
    }

    /**
     * Generates and returns a [PQLookupTable] for the given [VectorValue] and the given [DistanceKernel].
     *
     * @param v The [VectorValue] to obtain the [PQLookupTable] for.
     * @param kernel The [DistanceKernel] to apply.
     * @return The [PQLookupTable] for the given [VectorValue] and the [DistanceKernel]
     */
    fun getLookupTable(v: VectorValue<*>, kernel: DistanceKernel) = PQLookupTable(
        Array(this.numberOfSubspaces) { k ->
            val codebook = this.codebooks[k]
            DoubleArray(codebook.numberOfCentroids) {
                kernel.invoke(
                    v.subvector(k * this.dimensionsPerSubspace, dimensionsPerSubspace),
                    codebook[it]
                ).value
            }
        }
    )
}
