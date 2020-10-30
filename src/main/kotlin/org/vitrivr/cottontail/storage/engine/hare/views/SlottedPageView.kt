package org.vitrivr.cottontail.storage.engine.hare.views

import org.vitrivr.cottontail.storage.engine.hare.SlotId
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.basics.PageConstants
import org.vitrivr.cottontail.storage.engine.hare.basics.PageRef

/**
 * A [AbstractPageView] implementation for a slotted [Page] design.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class SlottedPageView : AbstractPageView() {

    companion object {
        /** A constant used to indicated, that a [SlotId] has been freed  */
        const val CONST_OFFSET_FREED = -1

        /** The size of a slotted [Page]'s header in bytes. */
        const val SIZE_HEADER = 12

        /**  The size of a slotted [Page] entry in bytes (one Int to encode offset). */
        const val SIZE_ENTRY = 4

        /** The offset into a slotted [Page]'s header to get the number of slots. */
        private const val HEADER_OFFSET_SLOTS = 4

        /** The offset into a slotted [Page]'s header to get the free space pointer. */
        private const val HEADER_OFFSET_FREE = 8

        /**
         * Initializes the given [Page] as [SlottedPageView]. Makes necessary type checks.
         *
         * @param page The [PageRef] to be initialized.
         *
         * @throws IllegalArgumentException If the provided [Page] is incompatible with this [SlottedPageView]
         */
        fun initialize(page: Page) {
            val type = page.getInt(0)
            require(type == PageConstants.PAGE_TYPE_UNINITIALIZED) { "Cannot initialize page of type $type as ${DirectoryPageView::class.java.simpleName} (type = ${PageConstants.PAGE_TYPE_DIRECTORY})." }
            page.putInt(0, PageConstants.PAGE_TYPE_SLOTTED)
            page.putInt(HEADER_OFFSET_SLOTS, 0)
            page.putInt(HEADER_OFFSET_FREE, page.size)
        }
    }

    /** The number of slots held by this [SlottedPageView]. */
    val slots: Int
        get() = this.page?.getInt(HEADER_OFFSET_SLOTS)
                ?: throw IllegalStateException("This SlottedPageView is not wrapping any page and can therefore not be used for interaction.")

    /** The pointer to the the beginning of the free space (counted from the end of the [Page]). */
    val freePointer: Int
        get() = this.page?.getInt(HEADER_OFFSET_FREE)
                ?: throw IllegalStateException("This SlottedPageView is not wrapping any page and can therefore not be used for interaction.")

    /** The pointer to the the beginning of the free space (counted from the end of the [Page]). */
    val freeSpace: Int
        get() = this.freePointer - this.slots * SIZE_ENTRY - SIZE_HEADER

    /** The [pageTypeIdentifier] for the [SlottedPageView]. */
    override val pageTypeIdentifier: Int
        get() = PageConstants.PAGE_TYPE_SLOTTED


    /**
     * Tries to wrap the given [Page] in this [SlottedPageView].
     *
     * @param page The [Page] that should be wrapped.
     * @return This [SlottedPageView]
     * @throws IllegalArgumentException If the provided [Page] is incompatible with this [SlottedPageView]
     */
    override fun wrap(page: Page): SlottedPageView {
        super.wrap(page)
        return this
    }

    /**
     * The offset in bytes into the [Page] for the given [SlotId].
     *
     * @param slotId The [SlotId] to look-up the offset for.
     * @return The offset in bytes. If slot has been freed [CONST_OFFSET_FREED] is returned.
     */
    fun offset(slotId: SlotId): Int {
        check(this.page != null) { "This SlottedPageView is not wrapping any page and can therefore not be used for interaction." }
        require(slotId < this.slots) { "SlotId $slotId is out of bound for slotted page (p = $this)." }
        return this.page!!.getInt(SIZE_HEADER + SIZE_ENTRY * slotId)
    }

    /**
     * Returns the size (in bytes) of the slot identified by the given [SlotId].
     *
     * @param slotId The [SlotId] to determine the size for.
     * @return The size in bytes.
     */
    fun size(slotId: SlotId): Int {
        check(this.page != null) { "This SlottedPageView is not wrapping any page and can therefore not be used for interaction." }
        val slots = this.slots
        require(slotId < slots) { "SlotId $slotId is out of bound for slotted page (p = $this)." }

        /* Check if slot has been freed. */
        val offset = this.offset(slotId)
        if (offset == CONST_OFFSET_FREED) return 0

        /* Determine size of slot. */
        return when (slotId.toInt()) {
            0 -> (this.page!!.size - this.offset(slotId))
            else -> this.offset((slotId - 1).toShort()) - this.offset(slotId)
        }
    }

    /**
     * Tries to allocate a new [SlotId] for the given number of bytes. If the method succeeds, a new
     * [SlotId] is returned the required space is allocated. Otherwise, the method returns null.
     *
     * @param size The number of bytes to allocate a [SlotId] for.
     * @return [SlotId] or null, if no [SlotId] could be allocated.
     */
    fun allocate(size: Int): SlotId? {
        check(this.page != null) { "This SlottedPageView is not wrapping any page and can therefore not be used for interaction." }

        val newFree = this.freePointer - size
        val newSlotId = this.slots
        if (newFree < SIZE_HEADER + (newSlotId + 1) * SIZE_ENTRY) return null
        this.page!!.putInt(SIZE_HEADER + SIZE_ENTRY * newSlotId, newFree)
        this.page!!.putInt(HEADER_OFFSET_SLOTS, (newSlotId + 1))
        this.page!!.putInt(HEADER_OFFSET_FREE, newFree)
        return newSlotId.toShort()
    }

    /**
     * Releases the given [SlotId]. This removes the [SlotId] from the slot registry necessarily
     * freeing up any space in the [SlottedPageView].
     *
     * @param slotId The [SlotId] to free up.
     */
    fun release(slotId: SlotId) {
        require(slotId < this.slots) { "SlotId $slotId is out of bound for slotted page (p = $this)." }
        this.page!!.putInt(HEADER_OFFSET_SLOTS, (this.slots - 1))
        this.page!!.putInt(SIZE_HEADER + SIZE_ENTRY * slotId, CONST_OFFSET_FREED)
        if (slotId == (this.slots - 1).toShort()) {
            this.page!!.putInt(HEADER_OFFSET_FREE, this.freePointer + this.size(slotId))
        }
    }
}

