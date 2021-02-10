package org.vitrivr.cottontail.database.queries.binding

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.Binding
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.recordset.StandaloneRecord

/**
 * A [Binding] for a [Record].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class RecordBinding(
    val tupleId: TupleId,
    columns: Array<ColumnDef<*>> = emptyArray(),
    values: Array<ValueBinding> = emptyArray()
) :
    Binding<Record> {

    /**
     *
     */
    constructor(tupleId: TupleId, collection: Collection<Pair<ColumnDef<*>, ValueBinding>>) : this(
        tupleId
    ) {
        collection.forEach {
            if (it.first.type != it.second.type) {
                throw IllegalArgumentException("Provided value ${it.first} is incompatible with column ${it.second}.")
            }
            this.map[it.first] = it.second
        }
    }

    /** Internal [Object2ObjectArrayMap] that holds all the mappings. */
    private val map = Object2ObjectArrayMap<ColumnDef<*>, ValueBinding>(columns, values)

    /** Number of [ColumnDef]s in this [RecordBinding]. */
    val size: Int
        get() = this.map.size

    /**
     * Returns bound [Record] for this [recover] given the [QueryContext].
     *
     * @param context [QueryContext] to use to obtain [Record] for this [RecordBinding].
     * @return [Record]
     */
    override fun bind(context: QueryContext): Record =
        StandaloneRecord(this.tupleId, this.map.map { it.key to it.value.bind(context) })

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as RecordBinding

        if (tupleId != other.tupleId) return false
        if (map != other.map) return false

        return true
    }

    override fun hashCode(): Int {
        var result = tupleId.hashCode()
        result = 31 * result + map.hashCode()
        return result
    }
}