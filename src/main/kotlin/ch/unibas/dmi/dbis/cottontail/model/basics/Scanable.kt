package ch.unibas.dmi.dbis.cottontail.model.basics

import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset

/**
 * An objects that holds [Record] values and allows for scanning operation on those [Record] values.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface Scanable {

    /**
     * Returns an [Iterable] of all tuple ID's used in this [Scanable].
     *
     * @return [Iterable] of all tuple IDs.
     */
    fun listTupleIds(): Iterator<Long>

    /**
     * Applies the provided action to each [Record] in this [Recordset].
     *
     * @param action The action that should be applied.
     */
    fun forEach(action: (Record) -> Unit)

    /**
     * Applies the provided mapping function to each [Record] in this [Filterable].
     *
     * @param action The mapping function that should be applied.
     * @return Collection of the results of the mapping function.
     */
    fun <R> map(action: (Record) -> R): Collection<R>
}