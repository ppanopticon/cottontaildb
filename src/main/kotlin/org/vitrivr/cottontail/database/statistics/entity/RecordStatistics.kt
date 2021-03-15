package org.vitrivr.cottontail.database.statistics.entity

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.model.values.types.Value
import java.util.*


/**
 * A collection of [ValueStatistics] for a record as used by the query planner.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
open class RecordStatistics {

    companion object {
        val EMPTY = RecordStatistics()
    }

    /** The map of [ColumnDef] to [ValueStatistics] mappings held by this [RecordStatistics]. */
    protected val columns = Object2ObjectOpenHashMap<ColumnDef<*>, ValueStatistics<Value>>()

    /**
     * Gets the [ValueStatistics] for the given [ColumnDef].
     *
     * @param key [ColumnDef]
     * @return [ValueStatistics]
     */
    operator fun get(key: ColumnDef<*>): ValueStatistics<*> = this.columns[key] ?: throw IllegalArgumentException("The column $key is not contained in this record statistics.")

    /**
     * Sets the [ValueStatistics] for the given [ColumnDef].
     *
     * @param key [ColumnDef]
     * @param statistics [ValueStatistics]
     */
    operator fun set(key: ColumnDef<*>, statistics: ValueStatistics<Value>) {
        require(!this.columns.containsKey(key)) { "Column $key is already contained in this record statistics." }
        this.columns[key] = statistics
    }

    /**
     * Sets the [ValueStatistics] for the given [ColumnDef].
     *
     * @param key [ColumnDef] to remove.
     */
    fun remove(key: ColumnDef<*>) = this.columns.remove(key)

    /**
     * Clears all [ValueStatistics] for this [RecordStatistics].
     */
    fun clear() = this.columns.clear()

    /**
     * Dumps all [ValueStatistics] to [ColumnDef] mappings contained in this [RecordStatistics].
     *
     * @return Unmodifiable [Map] of [ColumnDef] to [ValueStatistics] mappings.
     */
    fun all(): Map<ColumnDef<*>, ValueStatistics<Value>> = Collections.unmodifiableMap(this.columns)

    /**
     * Merges the other [RecordStatistics] into this [RecordStatistics], merging the [ColumnDef]s they contain.
     *
     * @param other [RecordStatistics] to merge with.
     * @return This [RecordStatistics]
     */
    fun combine(other: RecordStatistics): RecordStatistics {
        other.columns.forEach { (t, u) -> this[t] = u }
        return this
    }

    /**
     * Creates an exact copy of this [RecordStatistics].
     *
     * @return Copy of this [RecordStatistics].
     */
    open fun copy(): RecordStatistics {
        val copy = RecordStatistics()
        for ((t, u) in this.columns) {
            copy[t] = u
        }
        return copy
    }
}