package ch.unibas.dmi.dbis.cottontail.storage.store

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.util.concurrent.atomic.AtomicBoolean

/**
 * An abstract representation over a facility that can hold data (a data [Store]) and allows for random access and stores
 * the data it holds in a persistent fashion (i.e. backed by an I/O device).
 *
 * @version 1.0
 * @author Ralph Gasser
 */
abstract class PersistentStore : Store {

    /** A flag indicating whether or not this [PersistentStore] was closed. */
    protected var closed: AtomicBoolean = AtomicBoolean(false)

    /**
     * Forces all changes made to this [PersistentStore] to the underlying I/O device. Some [PersistentStore]s may not
     * support this, in which case they will always return false.
     *
     * @return State of the force action.
     */
    abstract fun force(): Boolean

    /**
     * Attempts to load all the data held by this [PersistentStore] into physical.
     */
    abstract fun load()

    /**
     * Returns true, if this [Store] is open and false otherwise.
     *
     * @return True if this [Store] is open, false otherwise.
     */
    override val isOpen: Boolean = !this.closed.get()

    /** Always returns true, since [PersistentStore] are persistent by definition. */
    override val isPersistent: Boolean
        get() = true

    /**
     * Generates and returns a [SeekableByteChannel] for this [PersistentStore]. The [SeekableByteChannel] can be used
     * to efficiently transfer data between other [ByteChannel][java.nio.channels.ByteChannel]s.
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class PersistentStoreChannel : SeekableByteChannel {
        @Volatile
        private var position = 0L

        override fun isOpen(): Boolean = !this@PersistentStore.closed.get()

        @Synchronized
        override fun position(): Long = this.position

        @Synchronized
        override fun position(newPosition: Long): SeekableByteChannel {
            require(newPosition < this@PersistentStore.size) { "New position $newPosition is out of bounds (size: ${this@PersistentStore.size})." }
            this.position = newPosition
            return this
        }
        @Synchronized
        override fun write(src: ByteBuffer): Int {
            val start = src.position()
            this@PersistentStore.putData(this.position, src)
            val written = src.position() - start
            this.position += written
            return written
        }

        @Synchronized
        override fun read(dst: ByteBuffer): Int {
            val start = dst.position()
            this@PersistentStore.getData(this.position, dst)
            val read = dst.position() - start
            this.position += read
            return read
        }

        @Synchronized
        override fun truncate(size: Long): SeekableByteChannel {
            this@PersistentStore.truncate(size)
            if (size < this.position) {
                this.position = size
            }
            return this
        }

        @Synchronized
        override fun size(): Long = this@PersistentStore.size

        override fun close() {
            /* Closing the MappedFileStore channel has no effect, since it is bound to the underlying MappedFileStore. */
        }
    }
}