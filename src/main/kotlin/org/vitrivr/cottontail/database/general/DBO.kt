package org.vitrivr.cottontail.database.general

import org.vitrivr.cottontail.model.basics.Name
import java.nio.file.Path

/**
 * A simple database object in Cottontail DB.
 *
 * Database objects are closeable. Furthermore, they have Cottontail DB specific attributes.
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
}