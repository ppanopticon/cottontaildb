package org.vitrivr.cottontail.database.index

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface IndexConfig {
    /**
     * Converts this [NoIndexConfig] to a [Map] representation.
     *
     * @return [Map] representation of this [NoIndexConfig].
     */
    fun toMap(): Map<String, String>
}