package org.vitrivr.cottontail.execution.operators.sort

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.model.basics.Record
import kotlin.math.sign

/**
 * A set of [Comparator] implementations to compare two [Record]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class RecordComparator : Comparator<Record> {

    /**
     * Compares two [Record]s based on a single [ColumnDef] that are not nullable.
     */
    class SingleNonNullColumnComparator(val c: ColumnDef<*>, val o: SortOrder) : RecordComparator() {
        init {
            require(!c.nullable) { "Column cannot be nullable for SingleNonNullColumnComparator but is." }
        }

        override fun compare(o1: Record, o2: Record): Int {
            return this.o * o1[this.c]!!.compareTo(o2[this.c]!!).sign
        }
    }

    /**
     * Compares two [Record]s based on a single [ColumnDef] that can be null.
     */
    class SingleNullColumnComparator(val c: ColumnDef<*>, val o: SortOrder) : RecordComparator() {
        override fun compare(o1: Record, o2: Record): Int = this.o * when {
            o1[this.c] == null && o2[this.c] == null -> 0
            o1[this.c] == null && o2[this.c] != null -> -1
            o1[this.c] != null && o2[this.c] == null -> 1
            else -> o1[this.c]!!.compareTo(o2[this.c]!!).sign
        }
    }

    /**
     * Compares two [Record]s based on a multiple [ColumnDef] that are not nullable.
     */
    class MultiNonNullColumnComparator(private val sortOn: Array<Pair<ColumnDef<*>, SortOrder>>) : RecordComparator() {
        init {
            require(!sortOn.any { it.first.nullable }) { "Columns cannot be nullable for SingleNonNullColumnComparator but are." }
        }

        override fun compare(o1: Record, o2: Record): Int {
            var comparison = 0
            for (c in this.sortOn) {
                comparison = c.second * (o1[c.first]!!.compareTo(o2[c.first]!!).sign)
                if (comparison != 0) break
            }
            return comparison
        }
    }

    /**
     * Compares two [Record]s based on a multiple [ColumnDef] that can be nullable.
     */
    class MultiNullColumnComparator(private val sortOn: Array<Pair<ColumnDef<*>, SortOrder>>) : RecordComparator() {
        override fun compare(o1: Record, o2: Record): Int {
            var comparison = 0
            for (c in this.sortOn) {
                val c1 = o1[c.first]
                val c2 = o2[c.first]
                comparison = c.second * when {
                    c1 == null && c2 == null -> 0
                    c1 == null && c2 != null -> -1
                    c1 != null && c2 == null -> 1
                    else -> c1!!.compareTo(c2!!).sign
                }
                if (comparison != 0) break
            }
            return comparison
        }
    }
}