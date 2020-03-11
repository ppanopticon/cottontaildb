package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk

import java.nio.file.Path


open class DiskManagerException(message: String, cause: Throwable? = null) : Exception(message, cause)

/**
 *
 */
class PageIdOutOfBoundException(pageId: PageId, diskManager: DiskManager) : DiskManagerException("The given page ID $pageId is out of bounds for this HARE file (file: ${diskManager.path.fileName}, pages: ${diskManager.pages}.")

class FileLockException(message: String, cause: Throwable? = null) : DiskManagerException(message, cause)