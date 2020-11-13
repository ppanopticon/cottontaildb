package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable
import org.mapdb.CottontailStoreWAL
import org.mapdb.DB
import org.mapdb.DBMaker
import org.mapdb.volume.FileChannelVol
import org.mapdb.volume.MappedFileVol
import org.mapdb.volume.VolumeFactory
import org.vitrivr.cottontail.utilities.math.BitUtil
import java.nio.file.Path

@Serializable
data class MapDBConfig(
        val enableMmap: Boolean = true, /* Option to to use memory mapped files for Map DB based entities. */
        val forceUnmap: Boolean = true, /* Option to force unmap memory mapped files for Map DB based entities. */
        val pageShift: Int = 22, /* Size of a page (size = 2^dataPageShift) for data pages; influences the allocation increment as well as number of mmap handles for memory mapped files. */
        val lockTimeout: Long = 1000L
) {

    /** Generates the [VolumeFactory] instance for this [MapDBConfig]. */
    val volumeFactory: VolumeFactory
        get() = if (this.enableMmap) {
            MappedFileVol.MappedFileFactory(this.forceUnmap, false)
        } else {
            FileChannelVol.FACTORY
        }

    /**
     * Prepares and returns a [CottontailStoreWAL] instance for the given [Path] using the current [MapDBConfig]
     *
     * @param path The [Path] to the file.
     * @return [CottontailStoreWAL] instance.
     */
    fun store(path: Path): CottontailStoreWAL = CottontailStoreWAL.make(
            file = path.toString(),
            volumeFactory = this.volumeFactory,
            allocateIncrement = 1L shl this.pageShift,
            fileLockWait = this.lockTimeout
    )

    /**
     * Prepares and returns a Map [DB] for the given [Path].
     *
     * @param path The [Path] for which to create the [DB].
     * @return The resulting [DB].
     */
    fun db(path: Path): DB {
        val maker = DBMaker
                .fileDB(path.toFile())
                .transactionEnable()
                .concurrencyScale(BitUtil.nextPowerOfTwo(Runtime.getRuntime().availableProcessors()))
                .allocateIncrement(1L shl this.pageShift)
        if (this.enableMmap) {
            maker.fileMmapEnableIfSupported()
            if (this.forceUnmap) {
                maker.cleanerHackEnable()
            }
        } else {
            maker.fileChannelEnable()
        }
        return maker.make()
    }
}