package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk

object Constants {
    /** Identifier of every HARE file. */
    val FILE_HEADER_IDENTIFIER = charArrayOf('H', 'A', 'R', 'E')

    /** Version of the HARE file. */
    const val FILE_HEADER_VERSION = 1.toByte()

    /** Flag for when the file's consistency can be considered OK. Exact semantic depends on [DiskManager] implementation.  */
    const val FILE_CONSISTENCY_OK = 0.toByte()

    /** Flag for when the file's consistency must be checked. Exact semantic depends on [DiskManager] implementation.  */
    const val FILE_SANITY_CHECK = 1.toByte()
}