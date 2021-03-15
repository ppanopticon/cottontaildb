package org.vitrivr.cottontail.database.general

import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import java.nio.file.Path

/**
 * A database object [DBO] in Cottontail DB (e.g. a schema, entity etc.). [DBO]s are identified by
 * a [Name] and usually part of a [DBO] hierarchy. Furthermore, they can be used to create [Tx]
 * objects that act on the [DBO].
 *
 * @version 1.2.0
 * @author Ralph Gasser
 */
interface DBO : AutoCloseable {
    /** The [Name] of this [DBO]. */
    val name: Name

    /** The parent DBO (if such exists). */
    val parent: DBO?

    /** The [Path] to the [DBO]'s main file OR folder. */
    val path: Path

    /** True if this [DBO] was closed, false otherwise. */
    val closed: Boolean

    /** The [DBOVersion] of this [DBO]. */
    val version: DBOVersion

    /**
     * Creates a new [Tx] for the given [TransactionContext].
     *
     * @param context [TransactionContext] to create [Tx] for.
     */
    fun newTx(context: TransactionContext): Tx
}