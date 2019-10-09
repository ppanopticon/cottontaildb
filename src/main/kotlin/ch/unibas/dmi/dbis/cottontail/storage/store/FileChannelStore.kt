package ch.unibas.dmi.dbis.cottontail.storage.store

import ch.unibas.dmi.dbis.cottontail.model.exceptions.StoreException
import org.mapdb.DBException
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.SeekableByteChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.min

/**
 * A [PersistentStore] backed by a [FileChannel] and a series of [MappedByteBuffer]s for data access. The store can handle
 * files of any size between 0 and [Long.MAX_VALUE] bytes. All data is written through the [FileChannel], however, read
 * access is facilitated by the [MappedByteBuffer].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FileChannelStore(val path: Path, val readOnly: Boolean, val lockTimeout: Long = 5000) : PersistentStore() {

    companion object {
        /** Byte value written to the EOF of a grown [MappedFileChannelStore] file. */
        const val EOF_BYTE = Byte.MIN_VALUE
    }

    /** An internal, thread local read buffer for primitive values. */
    private val buffer = ThreadLocal.withInitial{ ByteBuffer.allocateDirect(8) }

    /**The [FileChannel] used for data access. */
    private val fileChannel = if (this.readOnly) {
        FileChannel.open(this.path, StandardOpenOption.READ, StandardOpenOption.SPARSE)
    } else {
        FileChannel.open(this.path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.SPARSE, StandardOpenOption.CREATE)
    }

    /** Acquires a [FileLock] for file underlying this [FileChannel]. */
    private val fileLock = this.acquireFileLock(this.lockTimeout)

    /** The size of this [MappedFileChannelStore] in bytes. */
    override val size: Long
        get() = this.fileChannel.size()

    override fun getDouble(offset: Long): Double = this.getData(offset, this.buffer.get().position(0).limit(8)).double
    override fun putDouble(offset: Long, value: Double)= this.putData(offset, ByteBuffer.allocateDirect(8).putDouble(value).rewind())

    override fun getFloat(offset: Long): Float = this.getData(offset, this.buffer.get().position(0).limit(4)).float
    override fun putFloat(offset: Long, value: Float) = this.putData(offset, ByteBuffer.allocateDirect(4).putFloat(value).rewind())

    override fun getLong(offset: Long): Long =  this.getData(offset, this.buffer.get().position(0).limit(Long.SIZE_BYTES)).long
    override fun putLong(offset: Long, value: Long) = this.putData(offset, ByteBuffer.allocateDirect(Long.SIZE_BYTES).putLong(value).rewind())

    override fun getInt(offset: Long): Int = this.getData(offset, this.buffer.get().position(0).limit(Int.SIZE_BYTES)).int
    override fun putInt(offset: Long, value: Int) = this.putData(offset, ByteBuffer.allocateDirect(Int.SIZE_BYTES).putInt(value).rewind())

    override fun getShort(offset: Long): Short = this.getData(offset,this.buffer.get().position(0).limit(Short.SIZE_BYTES)).short
    override fun putShort(offset: Long, value: Short) = this.putData(offset, ByteBuffer.allocateDirect(Short.SIZE_BYTES).putShort(value).rewind())

    override fun getChar(offset: Long): Char = this.getData(offset, this.buffer.get().position(0).limit(Char.SIZE_BYTES)).char
    override fun putChar(offset: Long, value: Char) = this.putData(offset, ByteBuffer.allocateDirect(Char.SIZE_BYTES).putChar(value).rewind())

    override fun getByte(offset: Long): Byte = this.getData(offset, this.buffer.get().position(0).limit(Byte.SIZE_BYTES)).get()
    override fun putByte(offset: Long, value: Byte) = this.putData(offset, ByteBuffer.allocateDirect(Byte.SIZE_BYTES).put(value).rewind())

    override fun getData(offset: Long, dst: ByteBuffer): ByteBuffer {
        val oldPos = dst.position()
        this.fileChannel.read(dst, offset)
        return dst.position(oldPos)
    }
    override fun putData(offset: Long, src: ByteBuffer) {
        require(offset + (src.limit()-src.position()) <= this.size) { "Cannot write beyond size of MappedFileStore (requested: $offset + ${src.limit()-src.position()} bytes, available: $size bytes). Please call grow() first." }
        val oldPos = src.position()
        this.fileChannel.write(src, offset)
        src.position(oldPos)
    }

    /**
     * Forces all the changes to this [MappedFileChannelStore] to be written to disk.
     *
     * @return False, if this [MappedFileChannelStore] is read only.
     */
    override fun force(): Boolean {
        if (this.readOnly)
            return false
        this.fileChannel.force(true)
        return true
    }

    /**
     * Tries to load the data of all the slices into physical memory.
     *
     * <p> As the [MappedByteBuffer.load] method, this method makes a best effort to ensure
     * that, when it returns, this [MappedFileChannelStore]'s content is resident in physical memory.
     * Invoking this method may cause some number of page faults and I/O operations to
     * occur. </p>
     *
     * @return
     */
    override fun load() {

    }

    /**
     * Clears the given range and writes it with all zeros.
     *
     * @param range The offset to clear.
     */
    override fun clear(range: LongRange) {
        val data = ByteBuffer.allocateDirect(8_000_000)
        val toWrite = (range.last-range.first)
        var written = 0L
        while (written < toWrite) {
            val bytesToWrite = min(data.capacity().toLong(), (toWrite-written + 1L))
            this.fileChannel.write(data.rewind().limit(bytesToWrite.toInt()), written)
            written += bytesToWrite
        }
    }

    /**
     * Truncates this [MappedFileChannelStore] to the given size. May cause the memory mapping of the file to be adjusted.
     *
     * @param newSize The size to which to truncate this [MappedFileChannelStore].
     */
    override fun truncate(newSize: Long) {
       try {
           this.fileChannel.truncate(newSize)
       } catch (e: IOException) {
            throw DBException.VolumeIOError(e)
       }
    }

    /**
     * Grows this [MappedFileChannelStore] so as to accommodate at least the given offset in bytes. Calling this method will pre-allocate the required space and update the memory mapping
     *
     * @param newSize The size to which to grow this [MappedFileChannelStore].
     */
    override fun grow(newSize: Long) {
        if (newSize <= this.size) {
            return
        }

        /* Pre-clear file by appending a EOF byte. */
        try {
            this.fileChannel.write(ByteBuffer.allocate(1).put(EOF_BYTE).rewind(),  newSize-1)
        } catch (e: IOException) {
            throw DBException.VolumeIOError(e)
        }
    }

    /**
     * Closes this [MappedFileChannelStore].
     */
    override fun close() {
        if (!this.closed.compareAndSet(false, true))
            return

        try {
            /* Release lock and close FileChannel. */
            if (this.fileLock.isValid) {
                this.fileLock.release()
            }
        } catch (e: IOException) {
            throw StoreException.StoreIOException(this, e)
        }
    }

    /**
     * Tries to acquire a file lock of this [FileChannel] and returns it.
     *
     * @param timeout The amount of milliseconds to wait for lock.
     * @return lock The [FileLock] acquired.
     */
    private fun acquireFileLock(timeout: Long) : FileLock {
        val start = System.currentTimeMillis()
        do  {
            try {
                val lock = this.fileChannel.tryLock()
                if (lock != null) {
                    return lock
                } else {
                    Thread.sleep(100)
                }
            } catch (e: IOException) {
                throw StoreException.StoreIOException(this, e)
            }
        } while (System.currentTimeMillis() - start < timeout)
        throw StoreException.StoreLockException(this)
    }

    /**
     * Generates a string description of this [MappedFileChannelStore].
     */
    override fun toString(): String = "MappedFileStore{file= ${this.path}, size = ${this.size}}"

    /**
     * Generates and returns a [SeekableByteChannel] for this [MappedFileChannelStore]. The [SeekableByteChannel] can be used
     * to efficiently transfer data between other [ByteChannel]s
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class MappedFileStoreChannel : SeekableByteChannel {
        @Volatile
        private var position = 0L

        override fun isOpen(): Boolean = !this@FileChannelStore.closed.get()

        @Synchronized
        override fun position(): Long = this.position

        @Synchronized
        override fun position(newPosition: Long): SeekableByteChannel {
            require(newPosition < this@FileChannelStore.size) { "New position $newPosition is out of bounds (size: ${this@FileChannelStore.size})." }
            this.position = newPosition
            return this
        }
        @Synchronized
        override fun write(src: ByteBuffer): Int {
            val start = src.position()
            this@FileChannelStore.putData(this.position, src)
            val written = src.position() - start
            this.position += written
            return written
        }

        @Synchronized
        override fun read(dst: ByteBuffer): Int {
            val start = dst.position()
            this@FileChannelStore.getData(this.position, dst)
            val read = dst.position() - start
            this.position += read
            return read
        }

        @Synchronized
        override fun truncate(size: Long): SeekableByteChannel {
            this@FileChannelStore.truncate(size)
            if (size < this.position) {
                this.position = size
            }
            return this
        }

        @Synchronized
        override fun size(): Long = this@FileChannelStore.size

        override fun close() {
            /* Closing the MappedFileStore channel has no effect, since it is bound to the underlying MappedFileStore. */
        }
    }
}