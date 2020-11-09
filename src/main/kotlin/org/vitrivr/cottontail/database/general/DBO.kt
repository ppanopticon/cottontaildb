package org.vitrivr.cottontail.database.general

import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.model.basics.Name
import java.nio.file.Path

/**
 * A simple database object in Cottontail DB. Database objects are [AutoCloseable]s. Furthermore,
 * they have Cottontail DB specific attributes.
 *
 * @author Ralph Gasser
 * @version 1.0.0.
 */
interface DBO : AutoCloseable {
    /** The [Name] of this [DBO]. */
    val name: Name

    /** The [Path] to the [DBO]'s main file OR folder. */
    val path: Path

    /** The [Catalogue] instance used by this [DBO]*/
    val catalogue: Catalogue

    /** True if this [DBO] was closed, false otherwise. */
    val closed: Boolean
}