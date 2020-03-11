package ch.unibas.dmi.dbis.cottontail.database.general

import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import java.nio.file.Path

/**
 * A simple database object in Cottontail DB.
 *
 * Database objects are closeable. Furthermore, they have Cottontail DB specific attributes.
 */
interface DBO : AutoCloseable {
    /** The simple [Name] of this [DBO]. */
    val name: Name

    /** The fully qualified [Name] of this [DBO]. */
    val fqn: Name

    /** The parent DBO (if such exists). */
    val parent: DBO?

    /** The [Path] to the [DBO]'s main file OR folder. */
    val path: Path

    /** True if this [DBO] was closed, false otherwise. */
    val closed: Boolean
}