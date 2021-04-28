package org.vitrivr.cottontail.utilities.io

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import kotlin.streams.toList

/**
 * Some utility functions for handling files and folders.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object TxFileUtilities {

    /** Separator used to separate the proper name of a Cottontail DB file or folder and transaction information. */
    const val TX_SEPARATOR = "~"

    /** Separator used to separate the proper name of a Cottontail DB file or folder and transaction information. */
    const val TX_CREATED = "${TX_SEPARATOR}created"

    /** Separator used to separate the proper name of a Cottontail DB file or folder and transaction information. */
    const val TX_DELETED = "${TX_SEPARATOR}deleted"

    /** */
    private val logger: Logger = LoggerFactory.getLogger(TxFileUtilities::class.java)

    /**
     * Normalizes a [Path] by checking and removing all parts of the file name that come after a [TX_SEPARATOR].
     * If the given [Path]'s filename does not contain any [TX_SEPARATOR] then nothing changes and the original
     * [Path] is returned
     *
     * @param path [Path] the [Path] to check.
     * @return Normalized path.
     */
    fun plainPath(path: Path): Path = if (path.fileName.toString().contains(TX_SEPARATOR)) {
        val normalizedName = path.fileName.toString().split(TX_SEPARATOR)[0]
        val normalizedPath = path.parent.resolve(normalizedName)
        normalizedPath
    } else {
        path
    }

    /**
     * Generates and returns a CREATE path for the given path.
     *
     * @param path [Path] the [Path] to generate CREATE path for.
     * @return CREATE path.
     */
    fun createPath(path: Path): Path = path.parent.resolve("${path.fileName}${TX_CREATED}${UUID.randomUUID()}")

    /**
     * Deletes the given file or folder and all files and folders it contains.
     *
     * @param path [Path] the [Path] to delete.
     */
    fun delete(path: Path) {
        if (Files.exists(path)) {
            Files.walk(path).sorted(Comparator.reverseOrder()).forEach {
                try {
                    Files.delete(it)
                } catch (e: IOException) {
                    this.logger.warn("Failed to delete $path (content: ${Files.list(path).toList().joinToString(",")}).")
                }
            }
        } else {
            this.logger.warn("Noting to delete at $path.")
        }
    }
}