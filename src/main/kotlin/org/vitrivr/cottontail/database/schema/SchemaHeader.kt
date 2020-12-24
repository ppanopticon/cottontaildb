package org.vitrivr.cottontail.database.schema

/**
 * The header of the [Schema]. Contains information regarding its content!
 *
 * @see Schema
 *
 * @author Ralph Gasser
 * @version 1.0f
 */
class SchemaHeader(val created: Long = System.currentTimeMillis(), var modified: Long = System.currentTimeMillis(), var entities: LongArray = LongArray(0)) {
    companion object {
        /** The identifier that is used to identify a Cottontail DB [Schema] file. */
        internal const val IDENTIFIER: String = "COTTONT_SCM"

        /** The version of the Cottontail DB [Schema]  file. */
        internal const val VERSION: Short = 1
    }
}