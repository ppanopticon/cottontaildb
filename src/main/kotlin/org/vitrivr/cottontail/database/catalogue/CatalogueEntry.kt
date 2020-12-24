package org.vitrivr.cottontail.database.catalogue

/**
 * An entry in the [Catalogue] corresponding to a [Schema].
 *
 * @see [Catalogue]
 * @see [Schema]
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal data class CatalogueEntry(val name: String)