package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
@Serializable
data class HareConfig(
        val pageShift: Int = 22, /* Size of a page (size = 2^dataPageShift) for data pages; influences the allocation increment as well as number of mmap handles for memory mapped files. */
        val lockTimeout: Long = 1000L
)