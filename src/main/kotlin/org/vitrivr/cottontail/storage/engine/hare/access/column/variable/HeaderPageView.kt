package org.vitrivr.cottontail.storage.engine.hare.access.column.variable

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.access.column.directory.DirectoryPageView
import org.vitrivr.cottontail.storage.engine.hare.access.column.variable.VariableHareColumnFile.Companion.ROOT_ALLOCATION_PAGE_ID
import org.vitrivr.cottontail.storage.engine.hare.access.column.variable.VariableHareColumnFile.Companion.ROOT_DIRECTORY_PAGE_ID
import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.basics.PageConstants
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.storage.engine.hare.views.PageView

/**
 * The [HeaderPageView] of a [VariableHareColumnFile]. The [HeaderPageView] is usually located on
 * the first [HarePage] in the [VariableHareColumnFile] file.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
inline class HeaderPageView(override val page: Page) : PageView {

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

        /** The offset into the [HeaderPageView]'s header to obtain the maximum [TupleId]. */
        const val HEADER_OFFSET_MAXTID = 36

        /** The offset into the [HeaderPageView]'s header to get pointer to the last directory page. */
        const val HEADER_OFFSET_LDIRPTR = 44

        /** The offset into the [HeaderPageView]'s header to get pointer to the last slotted page (allocation page). */
        const val HEADER_OFFSET_ALLOCPTR = 52

        /** Bit Masks (for flags) */

        /** Mask for 'NULLABLE' bit in this [HeaderPageView]. */
        const val HEADER_MASK_NULLABLE = 1L shl 0

        /**
         * Initializes and wraps a [Page] for usage as a [HeaderPageView].
         *
         * @param page [Page] that should be wrapped.
         * @param columnDef The [ColumnDef] of the [VariableHareColumnFile] this [HeaderPageView] belongs to.
         */
        fun initialize(page: Page, columnDef: ColumnDef<*>) {
            val type = page.getInt(0)
            require(type == PageConstants.PAGE_TYPE_UNINITIALIZED) { "Cannot initialize page of type $type as ${DirectoryPageView::class.java.simpleName} (type = ${PageConstants.PAGE_TYPE_DIRECTORY})." }
            page.putInt(0, PageConstants.PAGE_TYPE_HEADER_VARIABLE_COLUMN)
            page.putInt(HEADER_OFFSET_TYPE, columnDef.type.ordinal)                                 /* 4: Type of column. See ColumnDef.forOrdinal() */
            page.putInt(HEADER_OFFSET_LSIZE, columnDef.type.logicalSize)                            /* 8: Logical size of column (for structured data types). */
            page.putLong(HEADER_OFFSET_FLAGS, if (columnDef.nullable) {                             /* 12: Flags. */
                (0L or HEADER_MASK_NULLABLE)
            } else {
                0L
            })
            page.putLong(HEADER_OFFSET_COUNT, 0L)                                             /* 20: Number of entries (count) in column. */
            page.putLong(HEADER_OFFSET_DELETED, 0L)                                           /* 28: Number of deleted entries in column. */
            page.putLong(HEADER_OFFSET_MAXTID, 0L)                                            /* 36: Maximum Tuple ID. */
            page.putLong(HEADER_OFFSET_LDIRPTR, ROOT_DIRECTORY_PAGE_ID)                             /* 44: Page ID of the last directory page. */
            page.putLong(HEADER_OFFSET_ALLOCPTR, ROOT_ALLOCATION_PAGE_ID)                           /* 52: Page ID of the last slotted page. */
        }
    }

    /** The logical size of the [ColumnDef] held by this [VariableHareColumnFile]. */
    val size: Int
        get() = this.page.getInt(HEADER_OFFSET_LSIZE)

    /** The [Type] held by this [VariableHareColumnFile]. */
    val type: Type<*>
        get() = Type.forOrdinal(this.page.getInt(HEADER_OFFSET_TYPE), this.size)

    /** Special flags set for this [VariableHareColumnFile], such as, nullability. */
    val flags: Long
        get() = this.page.getLong(HEADER_OFFSET_FLAGS)

    /** True if this [VariableHareColumnFile] supports null values. */
    val nullable: Boolean
        get() = ((this.flags and HEADER_MASK_NULLABLE) > 0L)

    /** The total number of entries in this [VariableHareColumnFile]. */
    var count: Long
        get() = this.page.getLong(HEADER_OFFSET_COUNT)
        set(v) {
            this.page.putLong(HEADER_OFFSET_COUNT, v)
        }

    /** The maximum [TupleId] for this [VariableHareColumnFile]. */
    var maxTupleId: TupleId
        get() = this.page.getLong(HEADER_OFFSET_MAXTID)
        set(v) {
            this.page.putLong(HEADER_OFFSET_MAXTID, v)
        }

    /** The number of deleted entries in this [VariableHareColumnFile]. */
    var deleted: Long
        get() = this.page.getLong(HEADER_OFFSET_DELETED)
        set(v) {
            this.page.putLong(HEADER_OFFSET_DELETED, v)
        }

    /** The [PageId] of the last directory [Page]. */
    var lastDirectoryPageId: PageId
        get() = this.page.getLong(HEADER_OFFSET_LDIRPTR)
        set(v) {
            this.page.putLong(HEADER_OFFSET_LDIRPTR, v)
        }

    /** The [PageId] in allocation [Page]. */
    var allocationPageId: PageId
        get() = this.page.getLong(HEADER_OFFSET_ALLOCPTR)
        set(v) {
            this.page.putLong(HEADER_OFFSET_ALLOCPTR, v)
        }
}