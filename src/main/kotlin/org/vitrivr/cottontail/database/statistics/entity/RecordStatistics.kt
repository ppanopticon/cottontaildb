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
 * @version 1.1.0
 */
open class RecordStatistics {

    companion object {
        val EMPTY = RecordStatistics()
    }

    /** Returns true if this [RecordStatistics] is considered fresh. */
    val fresh: Boolean
        get() = this.columns.values.all { it.fresh }

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
     * Resets this [RecordStatistics] and sets all its values to to the default value.
     */
    open fun reset() {
        this.columns.forEach { it.value.reset() }
    }

    /**
     * Dumps all [ValueStatistics] to [ColumnDef] mappings contained in this [RecordStatistics].
     *
     * @return Unmodifiable [Map] of [ColumnDef] to [ValueStatistics] mappings.
     */
    fun all(): Map<ColumnDef<*>, ValueStatistics<Value>> = Collections.unmodifiableMap(this.columns)

    /**
     * Creates an exact copy of this [RecordStatistics].
     *
     * @return Copy of this [RecordStatistics].
     */
    open fun copy(): RecordStatistics {
        val copy = RecordStatistics()
        for ((t, u) in this.columns) {
            copy[t] = u.copy()
        }
        return copy
    }
}