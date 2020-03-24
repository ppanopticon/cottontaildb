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

    /** The number of bits to shift in order to get the page size (i.e. the N in 2^N) . */
    val PAGE_BIT_SHIFT = System.getProperty("cottontail.page.shift")?.toIntOrNull() ?: 12

    /** The size of a [Page] as Int. This value is constant. */
    val PAGE_DATA_SIZE_BYTES = 1 shl PAGE_BIT_SHIFT

    /** The size of a [Page] as Long. This value is constant. */
    val PAGE_DATA_SIZE_BYTES_LONG = 1L shl PAGE_BIT_SHIFT

    /** The modulo mask of a [Page] as Int. Can be used to find the offset into a [Page] given an absolute position. This value is constant. */
    val PAGE_MOD_MASK = PAGE_DATA_SIZE_BYTES-1

    /** The modulo mask of a [Page] as Long. Can be used to find the offset into a [Page] given an absolute position. This value is constant. */
    val PAGE_MOD_MASK_LONG = PAGE_DATA_SIZE_BYTES-1L
}