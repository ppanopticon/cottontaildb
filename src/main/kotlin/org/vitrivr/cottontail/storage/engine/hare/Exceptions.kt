package org.vitrivr.cottontail.storage.engine.hare


open class DiskManagerException(message: String, cause: Throwable? = null) : Exception(message, cause)

/** Thrown whenever data in a HARE file is found to be corrupt. */
class DataCorruptionException(message: String) :  DiskManagerException(message)

/** */
class FileLockException(message: String, cause: Throwable? = null) : DiskManagerException(message, cause)