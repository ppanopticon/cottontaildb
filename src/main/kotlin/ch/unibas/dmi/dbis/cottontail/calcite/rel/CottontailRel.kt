package ch.unibas.dmi.dbis.cottontail.calcite.rel

import ch.unibas.dmi.dbis.cottontail.calcite.adapter.CottontailTable
import ch.unibas.dmi.dbis.cottontail.calcite.enumerators.Enumerators
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate

import org.apache.calcite.rel.RelNode
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.Convention

/**
 * @author Ralph Gasser
 * @version 1.0
 */
internal interface CottontailRel : RelNode {


    /** Calling convention for relational operations that occur in Cottontail DB.  */
    companion object {
        var CONVENTION: Convention = Convention.Impl("Cottontail", CottontailRel::class.java)
    }

    /**
     *
     */
    fun implement(implementor: Implementor)

    /** Callback for the implementation process that converts a tree of [CottontailRel] nodes into a Cottontail DB query. */
    class Implementor {
        /** Reference to the original [RelOptTable]. */
        var table: RelOptTable? = null

        /** Reference to the original [CottontailTable]. */
        var cottontailTable: CottontailTable? = null

        /** List of fields that should be projected to. */
        val projections: MutableMap<String,String> = mutableMapOf()

        /** List of [Predicate] that are connected by AND operations. */
        val predicates: MutableList<Predicate> = mutableListOf()

        /** The offset into the dataset. */
        var offset: Long = 0

        /** The number of items to fetch. */
        var limit: Long = Enumerators.LIMIT_NO_LIMIT

        /**
         * Adds a [Predicate] to this list of [Predicate]s.
         *
         * @param predicate [Predicate] to add.
         */
        fun addPredicate(predicate: Predicate) = this.predicates.add(predicate)

        /**
         * Adds a named column to the list of projection columns.
         *
         * @param column Name of the column to add.
         * @param alias The alias for the specified [ColumnDef].
         */
        fun addProjection(column: String, alias: String) = this.projections.put(column, alias)

        /**
         *
         */
        fun visitChild(ordinal: Int, input: RelNode) {
            assert(ordinal == 0)
            (input as CottontailRel).implement(this)
        }
    }
}