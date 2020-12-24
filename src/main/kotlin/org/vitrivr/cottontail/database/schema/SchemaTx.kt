package org.vitrivr.cottontail.database.schema

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.Tx
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name

/**
 * A [Tx] that operates on a single [Schema].
 *
 * [Tx]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface SchemaTx : Tx {
    /**
     * Returns a list of [Entity] held by this [Schema].
     *
     * @return [List] of all [Name.EntityName].
     */
    fun listEntities(): List<Name.EntityName>

    /**
     * Returns an instance of [Entity] if such an instance exists. If the [Entity] has been loaded before,
     * that [Entity] is re-used. Otherwise, the [Entity] will be loaded from disk.
     *
     * @param name Name of the [Entity] to access.
     * @return [Entity]
     */
    fun entityForName(name: Name.EntityName): Entity

    /**
     * Creates a new [Entity] in this [Schema].
     *
     * @param name The name of the [Entity] that should be created.
     */
    fun createEntity(name: Name.EntityName, vararg columns: ColumnDef<*>)

    /**
     * Drops an [Entity] in this [Schema].
     *
     * @param name The name of the [Entity] that should be dropped.
     */
    fun dropEntity(name: Name.EntityName)
}