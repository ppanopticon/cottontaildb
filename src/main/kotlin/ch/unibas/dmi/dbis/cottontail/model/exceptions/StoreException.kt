package ch.unibas.dmi.dbis.cottontail.model.exceptions

import ch.unibas.dmi.dbis.cottontail.storage.store.Store
import java.io.IOException

open class StoreException(message: String) : Throwable(message) {

    /**
     * Thrown whenever access to a [Store] fails due to an [IOException].
     *
     * @param store The [Store] that caused the error.
     * @param e The [IOException]
     */
    class StoreIOException(store: Store, e: IOException): StoreException("Storage access failed due to an IOException: ${e.message} $store.")

    /**
     * Thrown whenever access to a [Store] fails due to locking issues.
     *
     * @param store The [Store] that caused the error.
     */
    class StoreLockException(store: Store): StoreException("Storage access failed because necessary lock could not be obtained $store.")

    /**
     * Thrown whenever access is attempted beyond the boundaries of the [Store]
     *
     * @param store The [Store] that caused the error.
     * @param offset The requested offset into to [Store].
     */
    class StoreEOFException(store: Store, offset: Long): StoreException("Store access at pos=$offset is out of bounds $store.")
}