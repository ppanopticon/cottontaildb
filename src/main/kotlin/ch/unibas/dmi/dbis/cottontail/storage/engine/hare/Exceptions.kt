package ch.unibas.dmi.dbis.cottontail.storage.engine.hare

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.PageId


open class DiskManagerException(message: String, cause: Throwable? = null) : Exception(message, cause)

/** Thrown whenever data in a HARE file is found to be corrupt. */
class DataCorruptionException(message: String) :  DiskManagerException(message)

class FileLockException(message: String, cause: Throwable? = null) : DiskManagerException(message, cause)