package org.vitrivr.cottontail.database.index.vaplus

/**
 * see H. Ferhatosmanoglu, E. Tuncel, D. Agrawal, A. El Abbadi (2006): High dimensional nearest neighbor searching. Information Systems.
 */

import org.apache.commons.math3.linear.EigenDecomposition
import org.apache.commons.math3.linear.MatrixUtils
import org.apache.commons.math3.linear.RealMatrix
import org.apache.commons.math3.stat.correlation.Covariance
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.values.types.VectorValue
import java.io.Serializable
import java.util.*
import kotlin.collections.ArrayList
import kotlin.math.abs
import kotlin.math.pow

/**
 * This class implements the necessary methods for a VA+ index structure (see H. Ferhatosmanoglu, E. Tuncel, D. Agrawal,
 * A. El Abbadi (2006): High dimensional nearest neighbor searching. Information Systems).
 *
 * TODO: Fix and finalize implementation.
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class VAPlus : Serializable {

    private val totalNumberOfBits = 32 // 64, 128, 256, 1024

    /* Indexing */
    /**
     * This method generates a sample subset from the data.
     *
     * @param tx  An [Entity] containing all data.
     * @param size  The size of the data sample.
     * @return  An subset from data.
     */
    fun getDataSample(tx: EntityTx, columns: Array<ColumnDef<*>>, size: Int): Array<DoubleArray> {
        val p = size / tx.count().toDouble()
        val dataSample = ArrayList<DoubleArray>(size + 100)
        val random = SplittableRandom(System.currentTimeMillis())
        tx.scan(columns).forEach { record ->
            if (random.nextDouble() >= 1.0 - p) {
                dataSample.add(convertToDoubleArray(record[columns[0]] as VectorValue<*>))
            }
        }
        return dataSample.toTypedArray()
    }

    /**
     * "VA-file in KLT domain"
     * (see paper page 515-517)
     *
     * This method does a transformation into the KLT domain, in the paper described as "strip off all the correlation
     * between the dimensions of feature vectors", following the steps below:
     *
     * - Create a [RealMatrix] from data.
     * - Create a [Covariance] from [RealMatrix].
     * - Create a covariance matrix, cBar.
     * - Create a [EigenDecomposition] from cBar.
     * - Create an empty KLT matrix, k.
     * - Get eigenVector from column in cBar.
     * - In this column, fill the row with the corresponding eigenVector.
     * - Transpose k to kBar.
     * - Create a [RealMatrix], dataMatrix, of every column in data.
     * - Multiply kBar with dataMatrix.
     *
     * @param dataSample An [Array<DoubleArray>] containing data samples.
     * @return Pair of dataSample in KLT domain, and kBar.
     */
    fun transformToKLTDomain(dataSample: Array<DoubleArray>): Pair<Array<DoubleArray>, RealMatrix> {
        val cBar = Covariance(MatrixUtils.createRealMatrix(dataSample)).covarianceMatrix
        val eigenDecomposition = EigenDecomposition(cBar)
        val k = MatrixUtils.createRealMatrix(40, 40)
        for (column in 0 until cBar.columnDimension) {
            val eigenVector = eigenDecomposition.getEigenvector(column)
            for (row in 0 until cBar.rowDimension) {
                k.setEntry(row, column, eigenVector.getEntry(row))
            }
        }
        val kBar = k.transpose()
        dataSample.forEachIndexed { index, element ->
            val dataMatrix = MatrixUtils.createRealMatrix(arrayOf(element))
            dataSample[index] = kBar.multiply(dataMatrix.transpose()).getColumnVector(0).toArray()
        }
        return Pair(dataSample, kBar)
    }

    /**
     * "Non-uniform bit allocation"
     * (see paper page 517-518)
     *
     * This method allocates bits to every dimension in a non-uniform way.
     *
     * @param dataSample The data sample.
     * @param dimension Dimension of the data.
     * @return An [IntArray] containing the number of bits per dimension.
     */
    fun nonUniformBitAllocation(dataSample: Array<DoubleArray>, dimension: Int): IntArray {
        val mean = DoubleArray(dimension)
        val variance = DoubleArray(dimension)
        for (i in 0 until dimension) {
            for (index in dataSample.indices) {
                mean[i] += dataSample[index][i]
            }
            mean[i] = mean[i] / dataSample.size
        }
        for (i in 0 until dimension) {
            for (index in dataSample.indices) {
                variance[i] += (dataSample[index][i] - mean[i]).pow(2)
            }
            variance[i] = mean[i] / dataSample.size
        }
        // Weber / Böhm (2000): Trading Quality for Time with Nearest Neighbor Search
        val b0 = totalNumberOfBits
        var k = 0
        val b = IntArray(dimension) { 0 }
        while (k < b0) {
            val j = getMaxIndex(variance)
            b[j] += 1
            variance[j] = variance[j] / 4
            k += 1
        }
        return b
    }

    /**
     * "Non-uniform quantization"
     * (see paper page 518)
     *
     * This method performs a non-uniform quantization.
     *
     * @param dataSample The data sample.
     * @param b The number of bits per dimension.
     */
    fun nonUniformQuantization(dataSample: Array<DoubleArray>, b: IntArray): Array<DoubleArray> {
        val numberOfMarks = IntArray(b.size) {
            minOf(maxOf(1, 2 shl (b[it] - 1)), Short.MAX_VALUE.toInt())
        }
        return MarksGenerator.getNonUniformMarks(dataSample, numberOfMarks)
    }

    /**
     * This methods calculates the signature of the vector.
     * This method checks for every mark if the corresponding vector is beyond or not.
     * If so, mark before is the corresponding mark.
     *
     * @param vector The vector.
     * @param marks The marks.
     * @return An [IntArray] containing the signature of the vector.
     */
    fun getCells(vector: DoubleArray, marks: Array<DoubleArray>): IntArray = IntArray(vector.size) {
        val index = marks[it].indexOfFirst { i -> i >= vector[it] }
        if (index == -1) {
            marks[it].size - 2
        } else {
            index - 1
        }
    }

    /**
     * This helper method convert vector to double array.
     *
     * @param vector A [VectorValue].
     * @return A [DoubleArray] with the values from value.
     */
    fun convertToDoubleArray(vector: VectorValue<*>): DoubleArray {
        TODO()
        //return DoubleArray(vector.logicalSize * 2) {
        //    vector.getAsDouble(it)
        //}
    }


    /**
     * This helper method returns the index of the maximal value in an array.
     *
     * @param array The [DoubleArray].
     * @return An [Int] denoting the index of the maximal value.
     */
    private fun getMaxIndex(array: DoubleArray): Int {
        var maxValue = Double.MIN_VALUE
        var maxIndex = -1
        for (index in array.indices) {
            val element = array[index]
            if (element > maxValue) {
                maxValue = element
                maxIndex = index
            }
        }
        return maxIndex
    }

    /* Querying */
    /**
     * This method computes bounds.
     */
    fun computeBounds(vector: DoubleArray, marks: Array<DoubleArray>): Pair<Array<DoubleArray>, Array<DoubleArray>> {
        val lbounds = Array(marks.size) { DoubleArray(maxOf(0, marks[it].size - 1)) }
        val ubounds = Array(marks.size) { DoubleArray(maxOf(0, marks[it].size - 1)) }
        for (i in marks.indices) {
            val dimMarks = marks[i]
            val fvi = vector[i]
            dimMarks.asSequence().windowed(size = 2, partialWindows = false).forEachIndexed { j, it ->
                val weights = 1
                val d0fv1 = weights * abs(it[0] - fvi).pow(2) // TODO parametrize weights and n
                val d1fv1 = weights * abs(it[1] - fvi).pow(2)
                if (fvi < it[0]) {
                    lbounds[i][j] = d0fv1
                } else if (fvi > it[1]) {
                    lbounds[i][j] = d1fv1
                }
                if (fvi <= (it[0] + it[1]) / 2.toFloat()) {
                    ubounds[i][j] = d1fv1
                } else {
                    ubounds[i][j] = d0fv1
                }
            }
        }
        return Pair(lbounds, ubounds)
    }

    /**
     * This method compresses bounds.
     */
    fun compressBounds(bounds: Array<DoubleArray>): Pair<IntArray, FloatArray> {
        val lengths = bounds.map { it.size }
        val totalLength = lengths.sum()
        val cumLengths = {
            // compute cumulative sum of lengths
            val cumLengthsTmp = IntArray(lengths.size)
            var i = 0
            var cumSum = 0
            while (i < lengths.size - 1) {
                cumSum += lengths[i]
                cumLengthsTmp[i + 1] = cumSum
                i += 1
            }
            cumLengthsTmp
        }
        val newBounds = {
            val newBoundsTmp = FloatArray(totalLength)
            var i = 0
            var j: Int
            while (i < lengths.size) {
                j = 0
                while (j < lengths[i]) {
                    newBoundsTmp[cumLengths()[i] + j] = bounds[i][j].toFloat()
                    j += 1
                }
                i += 1
            }
            newBoundsTmp
        }
        return Pair(cumLengths(), newBounds())
    }

    fun scan(query: FloatArray, marks: Array<List<Double>>) {
        /**
        val cellsDistUDF = (boundsIndexBc: Broadcast[CompressedBoundIndex], boundsBoundsBc: Broadcast[CompressedBoundBounds]) => udf((cells: Seq[Short]) => {
        var bound: Distance = 0
        var idx = 0
        while (idx < cells.length) {
        val cellsIdx = if (cells(idx) < 0) {
        (Short.MaxValue + 1) * 2 + cells(idx)
        } else {
        cells(idx)
        }
        bound += boundsBoundsBc.value(boundsIndexBc.value(idx) + cellsIdx)
        idx += 1
        }
        bound
        })
        import ac.spark.implicits._
        val boundedData = data
        .withColumn("ap_lbound", cellsDistUDF(lbIndexBc, lbBoundsBc)(col(AttributeNames.featureIndexColumnName)))
        .withColumn("ap_ubound", cellsDistUDF(ubIndexBc, ubBoundsBc)(col(AttributeNames.featureIndexColumnName))) //note that this is computed lazy!
        val pk = this.pk.name.toString
        //local filtering
        val localRes = boundedData.coalesce(ac.config.defaultNumberOfPartitionsIndex)
        .mapPartitions(pIt => {
        //in here  we compute for each partition the k nearest neighbours and collect the results
        val localRh = new VAResultHandler(k)
        while (pIt.hasNext) {
        val current = pIt.next()
        localRh.offer(current, pk)
        }
        localRh.results.iterator
        })
        /*import ac.spark.implicits._
        val minUpperPart = localRes
        .mapPartitions(pIt => Seq(pIt.maxBy(_.ap_upper)).iterator).agg(min("ap_upper")).collect()(0).getDouble(0)
        val res = localRes.filter(_.ap_lower <= minUpperPart).toDF()*/
        val res = if (ac.config.vaGlobalRefinement || options.get("vaGlobal").map(_.toBoolean).getOrElse(false)) {
        // global refinement
        val globalRh = new VAResultHandler(k)
        val gIt = localRes.collect.iterator

        while (gIt.hasNext) {
        val current = gIt.next()
        globalRh.offer(current, pk)
        }
        ac.sqlContext.createDataset(globalRh.results).toDF()
        } else {
        localRes.toDF()
        }
        res*/
    }

}
