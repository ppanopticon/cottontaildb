package org.vitrivr.cottontail.execution.operators.projection

import com.google.common.hash.BloomFilter
import com.google.common.hash.Funnel
import com.google.common.hash.PrimitiveSink
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.*
import java.nio.charset.Charset

/**
 * An [Operator.PipelineOperator] used during query execution. It generates new [Record]s for
 * each incoming [Record] and removes / renames field according to the [fields] definition provided.
 *
 * Only produces a single [Record].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class SelectDistinctProjectionOperator(parent: Operator, fields: List<Pair<Name.ColumnName, Name.ColumnName?>>, expected: Long) : Operator.PipelineOperator(parent) {

    /** [Funnel] implementation for [Record]s. */
    object RecordFunnel : Funnel<Record> {
        override fun funnel(from: Record, into: PrimitiveSink) {
            from.forEach { _, value ->
                when (value) {
                    is BooleanValue -> into.putBoolean(value.value)
                    is ByteValue -> into.putByte(value.value)
                    is ShortValue -> into.putShort(value.value)
                    is IntValue -> into.putInt(value.value)
                    is LongValue -> into.putLong(value.value)
                    is FloatValue -> into.putFloat(value.value)
                    is DoubleValue -> into.putDouble(value.value)
                    is DateValue -> into.putLong(value.value)
                    is Complex32Value -> {
                        into.putFloat(value.real.value)
                        into.putFloat(value.imaginary.value)
                    }
                    is Complex64Value -> {
                        into.putDouble(value.real.value)
                        into.putDouble(value.imaginary.value)
                    }
                    is StringValue -> into.putString(value.value, Charset.defaultCharset())
                    is BooleanVectorValue -> value.data.forEach { into.putBoolean(it) }
                    is IntVectorValue -> value.data.forEach { into.putInt(it) }
                    is LongVectorValue -> value.data.forEach { into.putLong(it) }
                    is FloatVectorValue -> value.data.forEach { into.putFloat(it) }
                    is DoubleVectorValue -> value.data.forEach { into.putDouble(it) }
                    is Complex32VectorValue -> value.data.forEach { into.putFloat(it) }
                    is Complex64VectorValue -> value.data.forEach { into.putDouble(it) }
                    else -> into.putLong(-1L)
                }
            }
        }
    }

    /** True if names should be flattened, i.e., prefixes should be removed. */
    private val flattenNames = fields.all { it.first.schema() == fields.first().first.schema() }

    /** Parent [ColumnDef] to access and aggregate. */
    private val parentColumns = this.parent.columns.filter { c ->
        fields.any { f -> f.first.matches(c.name) }
    }

    /** The [BloomFilter] used for SELECT DISTINCT. */
    private val bloomFilter = BloomFilter.create(RecordFunnel, expected)

    /** Columns produced by [SelectProjectionOperator]. */
    override val columns: Array<ColumnDef<*>> = this.parent.columns.mapNotNull { c ->
        val match = fields.find { f -> f.first.matches(c.name) }
        if (match != null) {
            val alias = match.second
            when {
                alias != null -> c.copy(name = alias)
                this.flattenNames -> c.copy(name = Name.ColumnName(c.name.simple))
                else -> c
            }
        } else {
            null
        }
    }.toTypedArray()

    /** [SelectProjectionOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [SelectProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [SelectProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        return this.parent.toFlow(context).map { r ->
            StandaloneRecord(r.tupleId, this.columns, this.parentColumns.map { r[it] }.toTypedArray())
        }.filter {
            !this.bloomFilter.mightContain(it)
        }.onEach {
            this.bloomFilter.put(it)
        }
    }
}