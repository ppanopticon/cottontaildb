package org.vitrivr.cottontail.database.catalogue

import java.nio.file.Path
import java.util.*

/**
 * A [Catalogue] entry that describes a Cottontail DB schema.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class CatalogueSchemaEntry(val path: Path, val size: Int, val uuid: UUID)