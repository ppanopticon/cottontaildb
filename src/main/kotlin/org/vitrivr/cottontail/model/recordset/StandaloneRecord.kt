package org.vitrivr.cottontail.model.recordset

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import java.util.*

/**
 * A [Record] implementation as returned and processed by Cottontail DB. A [StandaloneRecord] can
 * exist without an enclosing [Recordset], i.e., each [StandaloneRecord] contains information
 * about the [ColumnDef] and the [Value]s it contains Internally, it uses a [Map] to store the values.
 *
 * <strong>Important:</strong> The use of [StandaloneRecord] is discouraged when data volume becomes
 * large, as each [StandaloneRecord] has its own reference to the [ColumnDef]s it contains.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class StandaloneRecord(
    override var tupleId: TupleId,
    columns: Array<ColumnDef<*>> = emptyArray(),
    values: Array<Value?> = emptyArray()
) : Record {

    /**
     *
     */
    constructor(
        tupleId: TupleId,
        collection: Collection<Pair<ColumnDef<*>, Value?>>
    ) : this(tupleId) {
        collection.forEach {
            if (!it.first.validate(it.second)) {
                throw IllegalArgumentException("Provided value ${it.first} is incompatible with column ${it.second}.")
            }
            this.map[it.first] = it.second
        }
    }

    /**
     * Constructor for single entry [StandaloneRecord].
     *
     * @param tupleId The [TupleId] of the [StandaloneRecord].
     * @param column The [ColumnDef] of the [StandaloneRecord].
     * @param value The [Value] of the [StandaloneRecord].
     */
    constructor(tupleId: TupleId, column: ColumnDef<*>, value: Value?) : this(tupleId) {
        if (!column.validate(value)) {
            throw IllegalArgumentException("Provided value $column is incompatible with column $value.")
        }
        this.map[column] = value
    }

    /** Internal [Object2ObjectLinkedOpenHashMap] that holds all the mappings. */
    private val map = Object2ObjectLinkedOpenHashMap<ColumnDef<*>, Value>(columns, values)

    /** Returns the [ColumnDef] held by this [StandaloneRecord]. */
    override val columns: Array<ColumnDef<*>>
        get() = this.map.keys.toTypedArray()

    /**
     * Copies this [StandaloneRecord] and returns the copy.
     *
     * @return Copy of this [StandaloneRecord]
     */
    override fun copy(): Record {
        val copy = StandaloneRecord(this.tupleId)
        copy.map.putAll(this.map)
        return copy
    }

    /**
     * Iterates over the [ColumnDef] and [Value] pairs in this [Record] in the order specified by [columns].
     *
     * @param action The action to apply to each [ColumnDef], [Value] pair.
     */
    override fun forEach(action: (ColumnDef<*>, Value?) -> Unit) =
        Object2ObjectMaps.fastForEach(this.map) { (c, v) ->
            action(c, v)
        }

    /**
     * Returns true, if this [StandaloneRecord] contains the specified [ColumnDef] and false otherwise.
     *
     * @param column The [ColumnDef] specifying the column
     * @return True if record contains the [ColumnDef], false otherwise.
     */
    override fun has(column: ColumnDef<*>): Boolean = this.map.contains(column)

    /**
     * Returns an unmodifiable [Map] of the data contained in this [StandaloneRecord].
     *
     * @return Unmodifiable [Map] of the data in this [StandaloneRecord].
     */
    override fun toMap(): Map<ColumnDef<*>, Value?> = Collections.unmodifiableMap(this.map)

    /**
     * Retrieves the value for the specified [ColumnDef] from this [StandaloneRecord].
     *
     * @param column The [ColumnDef] for which to retrieve the value.
     * @return The value for the [ColumnDef]
     */
    override fun get(column: ColumnDef<*>): Value? {
        require(this.map.contains(column)) { "The specified column ${column.name} (type=${column.type.name}) is not contained in this record." }
        return this.map[column]
    }

    /**
     * Sets the value for the specified [ColumnDef] in this [StandaloneRecord].
     *
     * @param column The [ColumnDef] for which to set the value.
     * @param value The new value for the [ColumnDef]
     */
    override fun set(column: ColumnDef<*>, value: Value?) {
        require(this.map.contains(column)) { "The specified column ${column.name} (type=${column.type.name})  is not contained in this record." }
        if (!column.validate(value)) {
            throw IllegalArgumentException("Provided value $value is incompatible with column $column.")
        }
        this.map[column] = value
    }


    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as StandaloneRecord

        if (tupleId != other.tupleId) return false
        if (this.map != other.map) return false

        return true
    }

    override fun hashCode(): Int {
        var result = tupleId.hashCode()
        result = 31 * result + map.hashCode()
        return result
    }
}