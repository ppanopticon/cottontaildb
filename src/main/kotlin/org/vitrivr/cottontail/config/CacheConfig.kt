package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable

/**
 * Config for Cottontail DB's various caches.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
@Serializable
data class CacheConfig(
    /** Number of query plans that should be cached. */
    val planCacheSize: Int = 100,

    /** Number of records that should be cached for batched inserts. */
    val insertCacheSize: Int = 1000
)