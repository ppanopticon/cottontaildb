package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk

object Constants {
    /** Identifier of every HARE file. */
    val FILE_HEADER_IDENTIFIER = charArrayOf('H', 'A', 'R', 'E')

    /** Version of the HARE file. */
    const val FILE_HEADER_VERSION = 1.toByte()

    /** Flag for when the file's sanity can be considered OK (i.e. it was closed correctly). */
    const val FILE_SANITY_OK = 0.toByte()

    /** Flag for when the file's sanity requires a check (i.e. it was not closed correctly). */
    const val FILE_SANITY_CHECK = 1.toByte()
}