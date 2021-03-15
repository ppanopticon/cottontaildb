package org.vitrivr.cottontail.database.schema

import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import java.nio.file.Path

/**
 * Represents a [Schema] in the Cottontail DB data model. A [Schema] is a collection of [Entity]
 * objects that belong together (e.g., because they belong to the same application). Every [Schema]
 * can be seen as a dedicated database and different [Schema]s in Cottontail can reside in
 * different locations.
 *
 * Calling the default constructor for [Schema] opens that [Schema]. It can only be opened once due
 * to file locks and it will remain open until the [Schema.close()] method is called.
 *
 * @see Entity
 * @see Column
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
interface Schema : DBO {
    /** The path to this [Schema] in the file system. */
    override val path: Path

    /** The [Catalogue] this [Schema] belongs to. */
    override val parent: Catalogue

    /** The [Name.SchemaName] of this [Schema]. */
    override val name: Name.SchemaName

    /** Flag indicating whether or not this [Schema] has been closed. */
    override val closed: Boolean

    /**
     * Creates and returns a new [SchemaTx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [Schema.Tx] for.
     * @return New [SchemaTx]
     */
    override fun newTx(context: TransactionContext): SchemaTx

    /**
     * Closes this [Schema] and all the [Entity] objects that are contained within.
     *
     * Since locks to [DBO] or [SchemaTx] objects may be held by other threads, it can take a
     * while for this method to complete.
     */
    override fun close()
}