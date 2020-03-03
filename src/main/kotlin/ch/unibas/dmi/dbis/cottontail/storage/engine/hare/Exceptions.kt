package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk

import java.nio.file.Path


open class DiskManagerException(message: String, cause: Throwable? = null) : Exception(message, cause)

class PageDoesNotExistException(message: String, cause: Throwable? = null) : DiskManagerException(message, cause)

class FileLockException(message: String, cause: Throwable? = null) : DiskManagerException(message, cause)