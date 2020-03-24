package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor

import java.nio.channels.ByteChannel
import java.nio.channels.SeekableByteChannel

/**
 * A cursor like data structure that can be used to address the raw bytes underlying a specific entry
 * in an arbitrary data structure (e.g. a file) using a [TupleId] between 1 and [Long.MAX_VALUE].
 *
 * [ByteCursor]s work similarly to [SeekableByteChannel]s with a few exceptions:
 *
 * - A [ByteCursor]'s absolute position can only be adjusted through [ByteCursor.tupleId].
 * - A [ByteCursor]'s absolute position can be queried through [ByteCursor.position]
 * - Calls to [ByteCursor.read] and [ByteCursor.write] move the absolute position of the [ByteCursor] WITHIN the entry addressed by [ByteCursor.tupleId]
 * - Calls to [ByteCursor.reset] can be used to reset the absolute position of the [ByteCursor] WITHIN the entry addressed by [ByteCursor.tupleId]
 * - It is possible to append data to a [ByteCursor] through [ByteCursor.append], which causes the underlying data structure the be extended
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface ByteCursor : ByteChannel, Iterator<TupleId> {

    /** The maximum [TupleId] supported by this [ByteCursor]. */
    val maximum: TupleId

    fun tupleId(): TupleId

    fun tupleId(new: TupleId)

    /**
     * Moves this [ByteCursor] to the next [TupleId]. Returns true on success and false, if this
     * [ByteCursor] doesn't support a next [TupleId]
     */
    fun previous(): TupleId

    /**
     * Appends a new entry to this [ByteCursor] effectively increasing [ByteCursor.maximum]. After
     * invocation of this method, [ByteCursor.position] should be equal to [ByteCursor.maximum]
     */
    fun append()
}