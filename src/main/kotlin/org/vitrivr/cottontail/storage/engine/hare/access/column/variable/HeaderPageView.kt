package org.vitrivr.cottontail.storage.engine.hare.access.column.variable

import org.vitrivr.cottontail.database.column.ColumnType
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.storage.engine.hare.DataCorruptionException
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.access.column.variable.VariableHareColumnFile.Companion.ROOT_ALLOCATION_PAGE_ID
import org.vitrivr.cottontail.storage.engine.hare.access.column.variable.VariableHareColumnFile.Companion.ROOT_DIRECTORY_PAGE_ID
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.DataPage
import org.vitrivr.cottontail.storage.engine.hare.views.AbstractPageView
import org.vitrivr.cottontail.storage.engine.hare.views.DirectoryPageView
import org.vitrivr.cottontail.storage.engine.hare.basics.PageConstants

/**
 * The [HeaderPageView] of a [VariableHareColumnFile]. The [HeaderPageView] is usually located on
 * the first [DataPage] in the [VariableHareColumnFile] file.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class HeaderPageView : AbstractPageView() {

    companion object {
        /** The offset into the [HeaderPageView]'s header to obtain type of column. */
        const val HEADER_OFFSET_TYPE = 4

        /** The offset into the [HeaderPageView]'s header to obtain logical size of column. */
        const val HEADER_OFFSET_LSIZE = 8

        /** The offset into the [HeaderPageView]'s header to obtain number file flags. */
        const val HEADER_OFFSET_FLAGS = 12

        /** The offset into the [HeaderPageView]'s header to obtain number of entries. */
        const val HEADER_OFFSET_COUNT = 20

        /** The offset into the [HeaderPageView]'s header to obtain number of deleted entries. */
        const val HEADER_OFFSET_DELETED = 28

        /** The offset into the [HeaderPageView]'s header to get pointer to the last [DirectoryPageView]. */
        const val HEADER_OFFSET_LDIRPTR = 36

        /** The offset into the [HeaderPageView]'s header to get pointer to the last [SlottedPageView] (allocation page). */
        const val HEADER_OFFSET_ALLOCPTR = 44

        /** Bit Masks (for flags) */

        /** Mask for 'NULLABLE' bit in this [HeaderPageView]. */
        const val HEADER_MASK_NULLABLE = 1L shl 0
    }

    /** The [pageTypeIdentifier] for the [HeaderPageView]. */
    override val pageTypeIdentifier: Int
        get() = PageConstants.PAGE_TYPE_HEADER_VARIABLE_COLUMN

    /** The [ColumnType] held by this [VariableHareColumnFile]. */
    val type: ColumnType<*>
        get() = ColumnType.forOrdinal(this.page?.getInt(HEADER_OFFSET_TYPE)
                ?: throw IllegalStateException("This HeaderPageView is not wrapping any page and can therefore not be used for interaction."))

    /** The logical size of the [ColumnDef] held by this [VariableHareColumnFile]. */
    val size: Int
        get() = this.page?.getInt(HEADER_OFFSET_LSIZE)
                ?: throw IllegalStateException("This HeaderView is not wrapping any page and can therefore not be used for interaction.")

    /** Special flags set for this [VariableHareColumnFile], such as, nullability. */
    val flags: Long
        get() = this.page?.getLong(HEADER_OFFSET_FLAGS)
                ?: throw IllegalStateException("This HeaderView is not wrapping any page and can therefore not be used for interaction.")

    /** True if this [VariableHareColumnFile] supports null values. */
    val nullable: Boolean
        get() = ((this.flags and HEADER_MASK_NULLABLE) > 0L)

    /** The total number of entries in this [VariableHareColumnFile]. */
    var count: Long
        get() = this.page?.getLong(HEADER_OFFSET_COUNT)
                ?: throw IllegalStateException("This HeaderView is not wrapping any page and can therefore not be used for interaction.")
        set(v) {
            check(this.page != null) { "This HeaderView is not wrapping any page and can therefore not be used for interaction." }
            this.page!!.putLong(HEADER_OFFSET_COUNT, v)
        }

    /** The number of deleted entries in this [VariableHareColumnFile]. */
    var deleted: Long
        get() = this.page?.getLong(HEADER_OFFSET_DELETED)
                ?: throw IllegalStateException("This HeaderView is not wrapping any page and can therefore not be used for interaction.")
        set(v) {
            check(this.page != null) { "This HeaderView is not wrapping any page and can therefore not be used for interaction." }
            this.page!!.putLong(HEADER_OFFSET_DELETED, v)
        }

    /** The [PageId] of the last directory [Page]. */
    var lastDirectoryPageId: PageId
        get() = this.page?.getLong(HEADER_OFFSET_LDIRPTR)
                ?: throw IllegalStateException("This HeaderView is not wrapping any page and can therefore not be used for interaction.")
        set(v) {
            check(this.page != null) { "This HeaderView is not wrapping any page and can therefore not be used for interaction." }
            this.page!!.putLong(HEADER_OFFSET_LDIRPTR, v)
        }

    /** The [PageId] in allocation [Page]. */
    var allocationPageId: PageId
        get() = this.page?.getLong(HEADER_OFFSET_ALLOCPTR)
                ?: throw IllegalStateException("This HeaderView is not wrapping any page and can therefore not be used for interaction.")
        set(v) {
            check(this.page != null) { "This HeaderView is not wrapping any page and can therefore not be used for interaction." }
            this.page!!.putLong(HEADER_OFFSET_ALLOCPTR, v)
        }

    /**
     * Wraps a [Page] for usage as a [HeaderPageView].
     *
     * @param page [Page] that should be wrapped.
     */
    override fun wrap(page: Page): HeaderPageView {
        super.wrap(page)
        require(this.count >= 0) { DataCorruptionException("Negative number of entries in HARE variable length column file.") }
        require(this.deleted >= 0) { DataCorruptionException("Negative number of deleted entries in HARE variable length column file.") }
        require(this.lastDirectoryPageId >= ROOT_DIRECTORY_PAGE_ID) { DataCorruptionException("Illegal page ID for last directory page.") }
        require(this.allocationPageId >= ROOT_ALLOCATION_PAGE_ID) { DataCorruptionException("Illegal page ID for last directory page.") }
        return this
    }

    /**
     * Using this method is prohibited; throws a [UnsupportedOperationException]
     *
     * @param page [Page] that should be wrapped.
     */
    @Deprecated("Usage of initializeAndWrap() without specifying a ColumnDef is prohibited for HeaderPageView.")
    override fun initializeAndWrap(page: Page): AbstractPageView {
        throw UnsupportedOperationException("Usage of initializeAndWrap() without specifying a ColumnDef is prohibited for HeaderPageView.")
    }

    /**
     * Initializes and wraps a [Page] for usage as a [HeaderPageView].
     *
     * @param page [Page] that should be wrapped.
     * @param columnDef The [ColumnDef] of the [VariableHareColumnFile] this [HeaderPageView] belongs to.
     */
    fun initializeAndWrap(page: Page, columnDef: ColumnDef<*>): HeaderPageView {
        super.initializeAndWrap(page)
        page.putInt(HEADER_OFFSET_TYPE, columnDef.type.ordinal)                                 /* 4: Type of column. See ColumnDef.forOrdinal() */
        page.putInt(HEADER_OFFSET_LSIZE, columnDef.logicalSize)                                 /* 8: Logical size of column (for structured data types). */
        page.putLong(HEADER_OFFSET_FLAGS, if (columnDef.nullable) {                             /* 12: Flags. */
            (0L or HEADER_MASK_NULLABLE)
        } else {
            0L
        })
        page.putLong(HEADER_OFFSET_COUNT, 0L)                                             /* 20: Number of entries (count) in column. */
        page.putLong(HEADER_OFFSET_DELETED, 0L)                                           /* 28: Number of deleted entries in column. */
        page.putLong(HEADER_OFFSET_LDIRPTR, ROOT_DIRECTORY_PAGE_ID)                             /* 36: Page ID of the last directory page. */
        page.putLong(HEADER_OFFSET_ALLOCPTR, ROOT_ALLOCATION_PAGE_ID)                           /* 44: Page ID of the last slotted page. */
        return this
    }
}