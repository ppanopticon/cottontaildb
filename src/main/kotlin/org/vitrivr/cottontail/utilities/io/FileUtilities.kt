package org.vitrivr.cottontail.utilities.io

import java.nio.file.Files
import java.nio.file.Path
import java.util.*

/**
 * Some utility functions for handling files and folders.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object FileUtilities {

    /**
     * Deletes all files and folders under the given path recursively (equivalent to rm -r).
     *
     * @param path The file or folder to delete.
     */
    fun deleteRecursively(path: Path) = Files.walk(path)
        .sorted(Comparator.reverseOrder())
        .forEach { Files.delete(it) }
}