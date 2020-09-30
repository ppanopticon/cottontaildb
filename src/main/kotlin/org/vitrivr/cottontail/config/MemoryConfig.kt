package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable
import org.mapdb.CottontailDBVolume

@Serializable
data class MemoryConfig(
        val forceUnmapMappedFiles: Boolean = true,
        val dataPageShift: Int = 22, /* Size of a page (size = 2^dataPageShift) for data pages; influences the allocation increment as well as number of mmap handles. */
        val cataloguePageShift: Int = 20 /* Size of a page (size = 2^cataloguePageShift) for catalogue pages; influences the allocation increment as well as number of mmap handles. */
) {
    /**
     * Getter for [CottontailDBVolume.CottontailDBVolumeFactory].
     */
    val volumeFactory
        get() = CottontailDBVolume.CottontailDBVolumeFactory(this.forceUnmapMappedFiles)
}