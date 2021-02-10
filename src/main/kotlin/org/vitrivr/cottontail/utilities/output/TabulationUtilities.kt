package org.vitrivr.cottontail.utilities.output

import com.jakewharton.picnic.Table
import com.jakewharton.picnic.TableSectionDsl
import com.jakewharton.picnic.table
import org.vitrivr.cottontail.database.queries.binding.extensions.fqn
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.util.*

/**
 * Utility class for tabulated output.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object TabulationUtilities {

    /**
     * Takes a [Iterator] of [CottontailGrpc.QueryResponseMessage], iterates over it and arranges
     * the results in a [Table].
     *
     * @param result The [Iterator] of [CottontailGrpc.QueryResponseMessage] to go visualize.
     */
    fun tabulate(result: Iterator<CottontailGrpc.QueryResponseMessage>): Table {
        var next = result.next()
        return table {
            cellStyle {
                border = true
                paddingLeft = 1
                paddingRight = 1
            }
            header {
                row {
                    next.columnsList.forEach { cell(it.fqn()) }
                }
            }
            body {
                next.tuplesList.forEach { tupleToRow(this, it) }
                while (result.hasNext()) {
                    next = result.next()
                    next.tuplesList.forEach { tupleToRow(this, it) }
                }
            }
        }
    }

    /**
     * Takes a  [CottontailGrpc.QueryResponseMessage] and arranges its results in a [Table].
     *
     * @param result The [CottontailGrpc.QueryResponseMessage] to go visualize.
     */
    fun tabulate(result: CottontailGrpc.QueryResponseMessage): Table = table {
        cellStyle {
            border = true
            paddingLeft = 1
            paddingRight = 1
        }
        header {
            row {
                result.columnsList.forEach { cell(it.fqn()) }
            }
        }
        body {
            result.tuplesList.forEach {
                tupleToRow(this, it)
            }
        }
    }

    /**
     * Transforms an individual [CottontailGrpc.Literal] to a row in a table.
     *
     * @param table The [TableSectionDsl] to create the row for.
     * @param tuple The [CottontailGrpc.Literal] to transform.
     */
    private fun tupleToRow(table: TableSectionDsl, tuple: CottontailGrpc.QueryResponseMessage.Tuple) = table.row {
        tuple.dataList.map {
            when (it.dataCase) {
                CottontailGrpc.Literal.DataCase.BOOLEANDATA -> it.booleanData.toString()
                CottontailGrpc.Literal.DataCase.INTDATA -> it.intData.toString()
                CottontailGrpc.Literal.DataCase.LONGDATA -> it.longData.toString()
                CottontailGrpc.Literal.DataCase.FLOATDATA -> it.floatData.toString()
                CottontailGrpc.Literal.DataCase.DOUBLEDATA -> it.doubleData.toString()
                CottontailGrpc.Literal.DataCase.STRINGDATA -> it.stringData
                CottontailGrpc.Literal.DataCase.DATEDATA -> Date(it.dateData.utcTimestamp).toString()
                CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> "${it.complex32Data.real} + i${it.complex32Data.imaginary}"
                CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> "${it.complex32Data.real} + i${it.complex32Data.imaginary}"
                CottontailGrpc.Literal.DataCase.VECTORDATA -> when (it.vectorData.vectorDataCase) {
                    CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> this@TabulationUtilities.vectorToString(it.vectorData.floatVector.vectorList)
                    CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> this@TabulationUtilities.vectorToString(it.vectorData.doubleVector.vectorList)
                    CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> this@TabulationUtilities.vectorToString(it.vectorData.intVector.vectorList)
                    CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> this@TabulationUtilities.vectorToString(it.vectorData.longVector.vectorList)
                    CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> this@TabulationUtilities.vectorToString(it.vectorData.boolVector.vectorList)
                    CottontailGrpc.Vector.VectorDataCase.COMPLEX32VECTOR -> this@TabulationUtilities.vectorToString(it.vectorData.complex32Vector.vectorList.map { c -> "${c.real} + i${c.imaginary}" })
                    CottontailGrpc.Vector.VectorDataCase.COMPLEX64VECTOR -> this@TabulationUtilities.vectorToString(it.vectorData.complex64Vector.vectorList.map { c -> "${c.real} + i${c.imaginary}" })
                    CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET -> "~~NULL~~"
                }
                CottontailGrpc.Literal.DataCase.NULLDATA -> "~~NULL~~"
                CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> "~~N/A~~"
                else -> ""
            }
        }.forEach { cell(it) }
    }

    /**
     * Concatenates a vector (list) into a [String]
     *
     * @param vector The [List] to concatenate.
     * @param max The maximum number of elements to include.
     */
    private fun vectorToString(vector: List<*>, max: Int = 4) = if (vector.size > max) {
        "[${vector.take(max - 1).joinToString(", ")}.., ${vector.last()}]"
    } else {
        "[${vector.joinToString(", ")}]"
    }
}