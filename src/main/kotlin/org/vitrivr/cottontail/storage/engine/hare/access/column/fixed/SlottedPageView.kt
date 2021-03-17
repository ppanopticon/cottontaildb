package org.vitrivr.cottontail.storage.engine.hare.access.column.fixed

import org.vitrivr.cottontail.storage.engine.hare.SlotId
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.buffer.BufferPool
import org.vitrivr.cottontail.storage.engine.hare.views.PageView
import kotlin.experimental.and
import kotlin.experimental.inv
import kotlin.experimental.or

/**
 * A [SlottedPageView] for a [FixedHareColumnFile]. A [SlottedPageView] usually contains data.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
inline class SlottedPageView(override val page: Page) : PageView {

    companion object {

        /** Mask for 'NULL' bit in each [FixedHareColumnFile] entry. */
        const val NO_FLAG = 0.toByte()

        /** Mask for 'NULL' bit in each [FixedHareColumnFile] entry. */
        const val NULL_FLAG_MASK = (1 shl 1).toByte()

        /** Mask for 'DELETED' bit in each [FixedHareColumnFile] entry. */
        const val DELETED_FLAG_MASK = (1 shl 2).toByte()
    }

    /**
     * Sets the [NO_FLAG] for the given [SlotId].
     *
     * @param slotId The [SlotId] to set [NO_FLAG] for.
     */
    fun setNoFlag(slotId: SlotId) {
        this.page.putByte(slotId, NO_FLAG)
    }

    /**
     * Reads the [NULL_FLAG_MASK] flag for the give [SlotId] on the given [BufferPool.BufferPoolPageRef]. This is a utility method.
     *
     * @param slotId The [SlotId] to read the flag for.
     * @return True if flag is set, false otherwise.
     */
    fun isNull(slotId: SlotId): Boolean {
        return this.page.getByte(slotId) and NULL_FLAG_MASK > 0
    }

    /**
     * Sets the [NULL_FLAG_MASK] flag for the give [SlotId] on the given [BufferPool.BufferPoolPageRef]. This is a utility method.
     *
     * @param slotId The [SlotId] to set the flag for.
     */
    fun setNull(slotId: SlotId) {
        this.page.putByte(slotId, page.getByte(slotId) or NULL_FLAG_MASK)
    }

    /**
     * Unsets the [NULL_FLAG_MASK] flag for the give [SlotId] on the given [BufferPool.BufferPoolPageRef]. This is a utility method.
     *
     * @param slotId The [SlotId] to set the flag for.
     */
    fun unsetNull(slotId: SlotId) {
        this.page.putByte(slotId, page.getByte(slotId) and NULL_FLAG_MASK.inv())
    }

    /**
     * Reads the [DELETED_FLAG_MASK] flag for the give [SlotId] on the given [BufferPool.BufferPoolPageRef]. This is a utility method.
     *
     * @param slotId The [SlotId] to read the flag for.
     * @return True if flag is set, false otherwise.
     */
    fun isDeleted(slotId: SlotId): Boolean {
        return this.page.getByte(slotId) and DELETED_FLAG_MASK > 0
    }

    /**
     * Sets the [DELETED_FLAG_MASK] flag for the give [SlotId] on the given [BufferPool.BufferPoolPageRef]. This is a utility method.
     *
     * @param slotId The [SlotId] to set the flag for.
     */
    fun setDeleted(slotId: SlotId) {
        this.page.putByte(slotId, page.getByte(slotId) or DELETED_FLAG_MASK)
    }

    /**
     * Unsets the [DELETED_FLAG_MASK] flag for the give [SlotId] on the given [BufferPool.BufferPoolPageRef]. This is a utility method.
     *
     * @param slotId The [SlotId] to set the flag for.
     */
    fun unsetDeleted(slotId: SlotId) {
        this.page.putByte(slotId, page.getByte(slotId) and DELETED_FLAG_MASK.inv())
    }
}