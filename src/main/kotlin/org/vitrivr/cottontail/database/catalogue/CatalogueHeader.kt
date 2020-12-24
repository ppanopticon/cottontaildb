package org.vitrivr.cottontail.database.catalogue

/**
 * The header section of the [Catalogue] data structure.
 *
 * @see [Catalogue]
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal data class CatalogueHeader(val size: Long = 0, val created: Long = System.currentTimeMillis(), val modified: Long = System.currentTimeMillis(), val schemas: LongArray = LongArray(0)) {
    companion object {
        /** The identifier that is used to identify a Cottontail DB [Catalogue] file. */
        internal const val IDENTIFIER: String = "COTTONT_CAT"

        /** The version of the Cottontail DB [Catalogue]  file. */
        internal const val VERSION: Short = 1
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CatalogueHeader

        if (size != other.size) return false
        if (created != other.created) return false
        if (modified != other.modified) return false
        if (!schemas.contentEquals(other.schemas)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = size.hashCode()
        result = 31 * result + created.hashCode()
        result = 31 * result + modified.hashCode()
        result = 31 * result + schemas.contentHashCode()
        return result
    }
}