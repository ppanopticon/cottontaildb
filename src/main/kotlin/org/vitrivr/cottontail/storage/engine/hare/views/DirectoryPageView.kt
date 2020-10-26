package org.vitrivr.cottontail.storage.engine.hare.views

import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.storage.engine.hare.Address
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.basics.PageConstants

/**
 * A [AbstractPageView] implementation for a directory [Page] that maps [TupleId] to [Address].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
open class DirectoryPageView : AbstractPageView() {

    companion object {
        /** <strong>Offsets</strong> */

        /** The offset into a [DirectoryPageView]'s header to get pointer to the previous [DirectoryPageView]. */
        const val HEADER_OFFSET_PREVPTR = 4

        /** The offset into a [DirectoryPageView]'s header to get pointer to the next [DirectoryPageView]. */
        const val HEADER_OFFSET_NEXTPTR = 12

        /** The offset into a [DirectoryPageView]'s header to get pointer to the first [TupleId]. */
        const val HEADER_OFFSET_TIDSTART = 20

        /** The offset into a [DirectoryPageView]'s header to get pointer to the last [TupleId]. */
        const val HEADER_OFFSET_TIDEND = 28

        /** <strong>Sizes</strong>  */

        /** Size of a [VariableHareColumnFile.Directory] page header in bytes. */
        const val DIRECTORY_HEADER_SIZE = 36

        /** Size of the [Address] part of a [VariableHareColumnFile.Directory] entry. */
        const val DIRECTORY_ENTRY_ADDRESS_SIZE = Long.SIZE_BYTES

        /** Size of the [Flags] part of a [VariableHareColumnFile.Directory] entry. */
        const val DIRECTORY_ENTRY_FLAGS_SIZE = Int.SIZE_BYTES

        /** Size of a [VariableHareColumnFile.Directory] entry. */
        const val DIRECTORY_ENTRY_SIZE = DIRECTORY_ENTRY_ADDRESS_SIZE + DIRECTORY_ENTRY_FLAGS_SIZE

        /** <strong>Constants</strong>  */

        /** Constant value for a NO_REF value. */
        const val NO_REF = -1L
    }

    /** The [pageTypeIdentifier] for the [SlottedPageView]. */
    override val pageTypeIdentifier: Int
        get() = PageConstants.PAGE_TYPE_DIRECTORY

    /** The [PageId] of the previous [DirectoryPageView] page. */
    var previousPageId: PageId
        get() = this.page?.getLong(HEADER_OFFSET_PREVPTR)
                ?: throw IllegalStateException("This DirectoryPageView is not wrapping any page and can therefore not be used for interaction.")
        private set(pageId) {
            check(this.page != null) { "This DirectoryPageView is not wrapping any page and can therefore not be used for interaction." }
            this.page!!.putLong(HEADER_OFFSET_PREVPTR, pageId)
        }

    /** The [PageId] of the next [DirectoryPageView] page. */
    var nextPageId: PageId
        get() = this.page?.getLong(HEADER_OFFSET_NEXTPTR)
                ?: throw IllegalStateException("This DirectoryPageView is not wrapping any page and can therefore not be used for interaction.")
        set(pageId) {
            check(this.page != null) { "This DirectoryPageView is not wrapping any page and can therefore not be used for interaction." }
            this.page!!.putLong(HEADER_OFFSET_NEXTPTR, pageId)
        }


    /** The first [TupleId] currently covered by this [DirectoryPageView]. */
    var firstTupleId: TupleId
        get() = this.page?.getLong(HEADER_OFFSET_TIDSTART)
                ?: throw IllegalStateException("This DirectoryPageView is not wrapping any page and can therefore not be used for interaction.")
        private set(tupleId) {
            check(this.page != null) { "This DirectoryPageView is not wrapping any page and can therefore not be used for interaction." }
            this.page!!.putLong(HEADER_OFFSET_TIDSTART, tupleId)
        }

    /** The last [TupleId] currently covered by this [DirectoryPageView]. */
    var lastTupleId: TupleId
        get() = this.page?.getLong(HEADER_OFFSET_TIDEND)
                ?: throw IllegalStateException("This DirectoryPageView is not wrapping any page and can therefore not be used for interaction.")
        private set(tupleId) {
            check(this.page != null) { "This DirectoryPageView is not wrapping any page and can therefore not be used for interaction." }
            this.page!!.putLong(HEADER_OFFSET_TIDEND, tupleId)
        }

    /** True, if this [DirectoryPageView] page is full. */
    val full: Boolean
        get() = DIRECTORY_HEADER_SIZE + (this.lastTupleId - this.firstTupleId + 2) * DIRECTORY_ENTRY_SIZE >= (this.page?.size
                ?: throw IllegalStateException("This DirectoryPageView is not wrapping any page and can therefore not be used for interaction."))

    /**
     * Returns the [Flags] for the given [TupleId].
     *
     * @param tupleId The [TupleId] to look-up.
     * @return [Flags] for [TupleId]
     */
    fun getFlags(tupleId: TupleId): Flags {
        check(this.page != null) { "This DirectoryPageView is not wrapping any page and can therefore not be used for interaction." }
        require(tupleId in this.firstTupleId..this.lastTupleId) { "TupleId $tupleId is out of bound for this directory page." }
        val offset = DIRECTORY_HEADER_SIZE + ((tupleId - this.firstTupleId).toInt()) * DIRECTORY_ENTRY_SIZE
        return this.page!!.getInt(offset)
    }

    /**
     * Returns the [Address] for the given [TupleId].
     *
     * @param tupleId The [TupleId] to look-up.
     * @return [Flags] for [TupleId]
     */
    fun getAddress(tupleId: TupleId): Address {
        check(this.page != null) { "This DirectoryPageView is not wrapping any page and can therefore not be used for interaction." }
        require(tupleId in this.firstTupleId..this.lastTupleId) { "TupleId $tupleId is out of bound for this directory page." }
        val offset = DIRECTORY_HEADER_SIZE + ((tupleId - this.firstTupleId).toInt()) * DIRECTORY_ENTRY_SIZE + DIRECTORY_ENTRY_FLAGS_SIZE
        return this.page!!.getLong(offset)
    }

    /**
     * Returns true, if this [DirectoryPageView] contains the given [TupleId].
     *
     * @param tupleId The [TupleId] to check.
     * @return True if this [DirectoryPageView] contains the [TupleId], false otherwise.
     */
    fun has(tupleId: TupleId): Boolean = (tupleId in this.firstTupleId..this.lastTupleId)

    /**
     * Sets the [Flags] for the given [TupleId].
     *
     * @param tupleId [TupleId] to set flags for.
     * @param flags The [Flags] to set.
     */
    fun setFlags(tupleId: TupleId, flags: Flags) {
        check(this.page != null) { "This DirectoryPageView is not wrapping any page and can therefore not be used for interaction." }
        require(tupleId in this.firstTupleId..this.lastTupleId) { "TupleId $tupleId is out of bound for directory page." }
        val offset = DIRECTORY_HEADER_SIZE + ((tupleId - this.firstTupleId).toInt()) * DIRECTORY_ENTRY_SIZE
        this.page!!.putInt(offset, flags)
    }

    /**
     * Sets the [Address] for the given [TupleId].
     *
     * @param tupleId [TupleId] to set flags for.
     * @param address The [Address] to set.
     */
    fun setAddress(tupleId: TupleId, address: Address) {
        require(tupleId in this.firstTupleId..this.lastTupleId) { "TupleId $tupleId is out of bound for directory page." }
        val offset = DIRECTORY_HEADER_SIZE + ((tupleId - this.firstTupleId).toInt()) * DIRECTORY_ENTRY_SIZE + DIRECTORY_ENTRY_FLAGS_SIZE
        this.page!!.putLong(offset, address)
    }

    /**
     * Allocates a new [TupleId] with the given [Address] and [Flags].
     *
     * @param flags The [Flags] value for the [TupleId].
     * @param address The [Address] value for the [TupleId].
     */
    fun allocate(flags: Flags, address: Address): TupleId {
        check(this.page != null) { "This DirectoryPageView is not wrapping any page and can therefore not be used for interaction." }
        check(!this.full) { "This DirectoryPageView is full and can therefore not be used to allocate a new TupleId." }

        /* Determine new tuple ID. */
        val tupleId = if (this.lastTupleId == -1L) {
            this.firstTupleId
        } else {
            (++this.lastTupleId)
        }
        val flags_offset = DIRECTORY_HEADER_SIZE + ((tupleId - this.firstTupleId).toInt()) * DIRECTORY_ENTRY_SIZE
        val address_offset = flags_offset + DIRECTORY_ENTRY_FLAGS_SIZE

        /* Write information to page. */
        this.page!!.putLong(HEADER_OFFSET_TIDEND, tupleId)
        this.page!!.putInt(flags_offset, flags)
        this.page!!.putLong(address_offset, address)

        return tupleId
    }


    /**
     * Wraps the given [Page] in this [DirectoryPageView] and returns this [DirectoryPageView].
     *
     * @param page [Page] to wrap.
     * @return This [DirectoryPageView].
     */
    override fun wrap(page: Page): DirectoryPageView {
        super.wrap(page)
        return this
    }

    /**
     * Initializes and wraps the given [Page] for usage as [DirectoryPageView].
     *
     * @param page [Page] that should be wrapped.
     * @param previous The previous [PageId] to initialize the [Page] with.
     * @param start The start [TupleId] to initialize the [Page] with.
     */
    fun initializeAndWrap(page: Page, previous: PageId, start: TupleId): DirectoryPageView {
        super.initializeAndWrap(page)
        this.previousPageId = previous
        this.nextPageId = NO_REF
        this.firstTupleId = start
        this.lastTupleId = NO_REF
        return this
    }

    /**
     * Using this method is prohibited; throws a [UnsupportedOperationException]
     *
     * @param page [Page] that should be wrapped.
     */
    @Deprecated("Usage of initializeAndWrap() without specifying a previous PageId and start TupleId is prohibited.")
    override fun initializeAndWrap(page: Page): DirectoryPageView {
        throw UnsupportedOperationException("Usage of initializeAndWrap() without specifying a previous PageId and start TupleId is prohibited.")
    }
}