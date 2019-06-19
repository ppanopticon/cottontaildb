package ch.unibas.dmi.dbis.cottontail.math.knn.metrics


/**
 * Interface implemented by [DistanceFunction]s that can be used to calculate the distance between two vectors.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface DistanceFunction {

    /**
     * Estimation of the number of operations required per vector component.
     */
    val operations: Int

    /**
     * Calculates the weighted distance between two [FloatArray]s
     *
     * @param a First [FloatArray]
     * @param b Second [FloatArray]
     * @param weights The [FloatArray] containing the weights.
     *
     * @return Distance between a and b.
     */
    operator fun invoke(a: FloatArray, b: FloatArray, weights: FloatArray): Double

    /**
     * Calculates the weighted distance between two [DoubleArray]s
     *
     * @param a First [DoubleArray]
     * @param b Second [DoubleArray]
     * @param weights The [DoubleArray] containing the weights.
     *
     * @return Distance between a and b.
     */
    operator fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double

    /**
     * Calculates the weighted distance between two [LongArray]s
     *
     * @param a First [LongArray]
     * @param b Second [LongArray]
     * @param weights The [LongArray] containing the weights.
     *
     * @return Distance between a and b.
     */
    operator fun invoke(a: LongArray, b: LongArray, weights: FloatArray): Double

    /**
     * Calculates the weighted distance between two [IntArray]s
     *
     * @param a First [IntArray]
     * @param b Second [IntArray]
     * @param weights The [IntArray] containing the weights.
     *
     * @return Distance between a and b.
     */
    operator fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double

    /**
     * Calculates the distance between two [FloatArray]s
     *
     * @param a First [FloatArray]
     * @param b Second [FloatArray]
     * @return Distance between a and b.
     */
    operator fun invoke(a: FloatArray, b: FloatArray): Double

    /**
     * Calculates the distance between two [DoubleArray]s
     *
     * @param a First [DoubleArray]
     * @param b Second [DoubleArray]
     * @return Distance between a and b.
     */
    operator fun invoke(a: DoubleArray, b: DoubleArray): Double

    /**
     * Calculates the distance between two [LongArray]s
     *
     * @param a First [LongArray]
     * @param b Second [LongArray]
     * @return Distance between a and b.
     */
    operator fun invoke(a: LongArray, b: LongArray): Double

    /**
     * Calculates the distance between two [IntArray]s
     *
     * @param a First [IntArray]
     * @param b Second [IntArray]
     * @return Distance between a and b.
     */
    operator fun invoke(a: IntArray, b: IntArray): Double
}