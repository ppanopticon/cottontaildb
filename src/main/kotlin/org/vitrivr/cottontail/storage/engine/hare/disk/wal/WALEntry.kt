package org.vitrivr.cottontail.storage.engine.hare.disk.wal

import org.vitrivr.cottontail.storage.engine.hare.PageId
import org.vitrivr.cottontail.storage.engine.hare.basics.View
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

/**
 * A view on the entry of a [WriteAheadLog] file.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class WALEntry : View {

    companion object {
        /** Size of a [WALEntry] entry. */
        const val SIZE = 24

        /** Offsets */

        /** Offset into the [WALEntry] to access the [sequenceNumber]. */
        const val OFFSET_SEQUENCE_NUMBER = 0

        /** Offset into the [WALEntry] to access the [action]. */
        const val OFFSET_WAL_ACTION = 8

        /** Offset into the [WALEntry] to access the [pageId]. */
        const val OFFSET_PAGE_ID = 12

        /** Offset into the [WALEntry] to access the [payloadSize]. */
        const val OFFSET_PAYLOAD_SIZE = 20
    }

    /** The [ByteBuffer] backing this [WALEntry]. */
    override val buffer: ByteBuffer = ByteBuffer.allocateDirect(SIZE)

    /** The sequence number of this [WALEntry]. */
    var sequenceNumber: Long
        get() = this.buffer.getLong(OFFSET_SEQUENCE_NUMBER)
        set(v) {
            this.buffer.putLong(OFFSET_SEQUENCE_NUMBER, v)
        }

    /** The [WALAction] carried out by this [WALEntry]. */
    var action: WALAction
        get() = WALAction.values()[this.buffer.getInt(OFFSET_WAL_ACTION)]
        set(v) {
            this.buffer.putInt(OFFSET_WAL_ACTION, v.ordinal)
        }

    /** The [PageId] affected by this [WALEntry]. Only meaningful for [WALAction.UPDATE], [WALAction.FREE_REUSE] and [WALAction.FREE_TRUNCATE] */
    var pageId: PageId
        get() = this.buffer.getLong(OFFSET_PAGE_ID)
        set(v) {
            this.buffer.putLong(OFFSET_PAGE_ID, v)
        }

    /** The size of the size of the payload carried by this [WALEntry]*/
    var payloadSize: Int
        get() = this.buffer.getInt(OFFSET_PAYLOAD_SIZE)
        set(v) {
            this.buffer.putInt(OFFSET_PAYLOAD_SIZE, v)
        }

    /**
     * Reads the content of this [WALEntry] from disk.
     *
     * @param channel The [FileChannel] to read from.
     * @param position The position in the [FileChannel] to write to.
     */
    override fun read(channel: FileChannel, position: Long): WALEntry {
        channel.read(this.buffer.rewind(), position)
        return this
    }

    /**
     * Writes the content of this [WALEntry] to disk.
     *
     * @param channel The [FileChannel] to write to.
     * @param position The position in the [FileChannel] to write to.
     */
    override fun write(channel: FileChannel, position: Long): WALEntry {
        channel.write(this.buffer.rewind(), position)
        return this
    }
}