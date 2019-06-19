package ch.unibas.dmi.dbis.cottontail.calcite.knn

import ch.unibas.dmi.dbis.cottontail.Cottontail
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.math.knn.metrics.Distance
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.type.DoubleArrayType
import ch.unibas.dmi.dbis.cottontail.model.type.FloatArrayType
import ch.unibas.dmi.dbis.cottontail.model.type.IntArrayType
import ch.unibas.dmi.dbis.cottontail.model.type.LongArrayType
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import ch.unibas.dmi.dbis.cottontail.utilities.name.first
import ch.unibas.dmi.dbis.cottontail.utilities.name.last
import org.apache.calcite.schema.impl.TableFunctionImpl
import java.math.BigDecimal

/**
 * This class represents a table function invocation for kNN lookup using unary query vector without weights. It can be used by Apache Calcite as a [TableFunctionImpl].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class WeightedUnaryScanningKnn : AbstractScanningKnn() {
    /**
     * Executes the kNN lookup on the given input.
     *
     * @param source The data source. Usually an enumerable stemming from a cursor (i.e. another query).
     * @param query A list of query vectors. The kNN results is calculated for each query vector.
     * @param k The k in kNN, i.e. the top k results will be kept. Defaults to 250
     * @param distance The name of the distance function to use. See [Distance]. Defaults to L2
     */
    fun evaluate(source: Name, id_field: Name, vec_field: Name, query: List<BigDecimal>, weights: List<BigDecimal>, k: Int, distance: String) : CottontailKnnTable {

        /* Bind names to DBOs. */
        val entity = Cottontail.CATALOGUE!!.schemaForName(source.first()).entityForName(source.last())
        val idCol = entity.columnForName(id_field) ?: throw QueryException.QueryBindException("Could not find ID column '$id_field' on entity '$source' for kNN query.")
        val vecCol = entity.columnForName(vec_field) ?: throw QueryException.QueryBindException("Could not find ID column '$vec_field' on entity '$source' for kNN query.")

        /* Perform kNN lookup. */
        val tx = entity.Tx(readonly = true, columns = arrayOf(idCol, vecCol))
        val results = when(vecCol.type) {
            is DoubleArrayType -> doubleKnn(tx = tx, id_col = idCol, vec_col = vecCol as ColumnDef<DoubleArray>, k = k, distance = Distance.valueOf(distance), queries = listOf(DoubleArray(query.size) { query[it].toDouble() }), weights = listOf(DoubleArray(weights.size) { weights[it].toDouble() }))
            is FloatArrayType -> floatKnn(tx = tx, id_col = idCol, vec_col = vecCol as ColumnDef<FloatArray>, k = k,  distance = Distance.valueOf(distance), queries = listOf(FloatArray(query.size) { query[it].toFloat() }), weights = listOf(FloatArray(weights.size) { weights[it].toFloat() }))
            is LongArrayType -> longKnn(tx = tx, id_col = idCol, vec_col = vecCol as ColumnDef<LongArray>, k = k,  distance = Distance.valueOf(distance), queries = listOf(LongArray(query.size) { query[it].toLong() }), weights = listOf(FloatArray(weights.size) { weights[it].toFloat() }))
            is IntArrayType -> intKnn(tx = tx, id_col = idCol, vec_col = vecCol as ColumnDef<IntArray>, k = k, distance = Distance.valueOf(distance), queries = listOf(IntArray(query.size) { query[it].toInt() }), weights = listOf(FloatArray(weights.size) { weights[it].toFloat() }))
            else -> throw QueryException.QueryBindException("Column of type ${vecCol.type} is not supported for kNN queries.")
        }

        /* Return a Enumerator. */
        return CottontailKnnTable(results)
    }
}