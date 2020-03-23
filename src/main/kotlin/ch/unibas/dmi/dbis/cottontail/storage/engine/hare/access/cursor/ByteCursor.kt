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
interface ByteCursor : ByteChannel {

    /** Size of an individual entry in bytes */
    val entrySize: Int

    /** The maximum [TupleId] supported by this [ByteCursor]. */
    val maximum: TupleId

    /** Returns the [TupleId] this [ByteCursor] is currently pointing to. */
    fun tupleId(): TupleId

    /**
     * Sets the [TupleId] this [ByteCursor] is currently pointing to. Setting its value beyond
     * [ByteCursor.maximum] will cause a [IndexOutOfBoundsException] to be thrown.
     *
     * @param new New [TupleId] this [ByteCursor] should be pointing to.
     * @throws IndexOutOfBoundsException If [new] > [ByteCursor.maximum]
     */
    fun tupleId(new: TupleId)

    /**
     * Moves this [ByteCursor] to the next [TupleId]. Returns true on success and false, if this
     * [ByteCursor] doesn't support a next [TupleId]
     */
    fun next(): Boolean

    /**
     * Moves this [ByteCursor] to the next [TupleId]. Returns true on success and false, if this
     * [ByteCursor] doesn't support a next [TupleId]
     */
    fun previous(): Boolean

    /**
     * Appends a new entry to this [ByteCursor] effectively increasing [ByteCursor.maximum]. After
     * invocation of this method, [ByteCursor.position] should be equal to [ByteCursor.maximum]
     */
    fun append()

    /**
     * Absolute position of the [ByteCursor] in bytes relative to the beginning of the data structure
     * that is addressed by [ByteCursor]
     *
     * @return Absolute position of the [ByteCursor] in bytes
     */
    fun position(): Long

    /**
     * Number of bytes that are remaining for read/write WITHIN the entry addressed by [ByteCursor.tupleId]
     *
     * @return Number of bytes that remaining for read/write
     */
    fun remaining(): Long

    /** Resets the internal position to the start of the entry reference by [ByteCursor.tupleId]. */
    fun reset()
}