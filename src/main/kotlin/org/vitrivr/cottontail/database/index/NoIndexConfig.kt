package org.vitrivr.cottontail.database.index

/**
 * A [IndexConfig] that is empty.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object NoIndexConfig : IndexConfig {

    /**
     * Converts this [NoIndexConfig] to a [Map] representation.
     *
     * @return [Map] representation of this [NoIndexConfig].
     */
    override fun toMap() = emptyMap<String, String>()
}