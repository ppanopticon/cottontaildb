package ch.unibas.dmi.dbis.cottontail.storage.store

import ch.unibas.dmi.dbis.cottontail.model.exceptions.StoreException
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.optimisticRead
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.roundUp
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.shift
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write

import org.mapdb.DBException

import java.io.IOException
import java.nio.*
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.Path
import java.nio.file.StandardOpenOption

import java.util.concurrent.locks.StampedLock

import kotlin.math.min

/**
 * A [PersistentStore] backed by a [FileChannel] and a series of [MappedByteBuffer]s for data access. The store can handle
 * files of any size between 0 and [Long.MAX_VALUE] bytes. All data is written through the [FileChannel], however, read
 * access is facilitated by the [MappedByteBuffer].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class MappedFileChannelStore(val path: Path, val readOnly: Boolean, val forceUnmap: Boolean = true, val lockTimeout: Long = 5000) : PersistentStore(), MappableStore {

    companion object {
        /** Byte value written to the EOF of a grown [MappedFileChannelStore] file. */
        const val EOF_BYTE = Byte.MIN_VALUE
    }

    /**The [FileChannel] used for data access. */
    private val fileChannel = if (this.readOnly) {
        FileChannel.open(this.path, StandardOpenOption.READ, StandardOpenOption.SPARSE)
    } else {
        FileChannel.open(this.path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.SYNC, StandardOpenOption.SPARSE, StandardOpenOption.CREATE)
    }

    /** Acquires a [FileLock] for file underlying this [FileChannel]. */
    private val fileLock = this.acquireFileLock(this.lockTimeout)

    /** Internal lock used to mediate access to changes to the slicing. */
    private val sliceLock = StampedLock()

    /** The size of this [MappedFileChannelStore] in bytes. */
    override val size: Long
        get() = this.fileChannel.size()

    /** The number of bits used to address the slice. */
    @Volatile
    private var sliceShift = 20

    /** The size of an individual slice (in bytes). */
    @Volatile
    private var sliceSize = 1L shl this.sliceShift

    /** Mask that can be used to identify the the slice part of the offset. */
    @Volatile
    private var sliceSizeModMask = this.sliceSize - 1L

    /** Internal list of [ByteBuffer] slices for data access. Gets initialized upon creation of this [MappedFileChannelStore]. */
    @Volatile
    private var slices = emptyArray<MappedByteBuffer>()

    init {
        this.remap()
    }

    override fun getDouble(offset: Long): Double = this.sliceLock.optimisticRead { this.sliceForOffset(offset).getDouble((offset and this.sliceSizeModMask).toInt())  }
    override fun putDouble(offset: Long, value: Double)= this.putData(offset, ByteBuffer.allocateDirect(8).putDouble(value).rewind())

    override fun getFloat(offset: Long): Float = this.sliceLock.optimisticRead {  this.sliceForOffset(offset).getFloat((offset and this.sliceSizeModMask).toInt()) }
    override fun putFloat(offset: Long, value: Float) = this.putData(offset, ByteBuffer.allocateDirect(4).putFloat(value).rewind())

    override fun getLong(offset: Long): Long = this.sliceLock.optimisticRead {  this.sliceForOffset(offset).getLong((offset and this.sliceSizeModMask).toInt()) }
    override fun putLong(offset: Long, value: Long) = this.putData(offset, ByteBuffer.allocateDirect(Long.SIZE_BYTES).putLong(value).rewind())

    override fun getInt(offset: Long): Int = this.sliceLock.optimisticRead {  this.sliceForOffset(offset).getInt((offset and this.sliceSizeModMask).toInt()) }
    override fun putInt(offset: Long, value: Int) = this.putData(offset, ByteBuffer.allocateDirect(Int.SIZE_BYTES).putInt(value).rewind())

    override fun getShort(offset: Long): Short = this.sliceLock.optimisticRead {  this.sliceForOffset(offset).getShort((offset and this.sliceSizeModMask).toInt()) }
    override fun putShort(offset: Long, value: Short) = this.putData(offset, ByteBuffer.allocateDirect(Short.SIZE_BYTES).putShort(value).rewind())

    override fun getChar(offset: Long): Char = this.sliceLock.optimisticRead {  this.sliceForOffset(offset).getChar((offset and this.sliceSizeModMask).toInt()) }
    override fun putChar(offset: Long, value: Char) = this.putData(offset, ByteBuffer.allocateDirect(Char.SIZE_BYTES).putChar(value).rewind())

    override fun getByte(offset: Long): Byte = this.sliceLock.optimisticRead {  this.sliceForOffset(offset).get((offset and this.sliceSizeModMask).toInt()) }
    override fun putByte(offset: Long, value: Byte) = this.putData(offset, ByteBuffer.allocateDirect(Byte.SIZE_BYTES).put(value).rewind())

    override fun getData(offset: Long, dst: ByteBuffer): ByteBuffer = this.sliceLock.optimisticRead {
        val oldPos = dst.position()
        val slices = this.slicesForRange(offset..(offset + (dst.remaining())))
        for (slice in slices) {
            val pos = if (slice == slices.first()) {
                (offset and this.sliceSizeModMask).toInt()
            } else {
                0
            }
            dst.put(slice.duplicate().position(pos).limit(dst.remaining()))
        }
        dst.position(oldPos)
        return dst
    }

    override fun putData(offset: Long, src: ByteBuffer) {
        require(offset + (src.limit()-src.position()) <= this.size) { "Cannot write beyond size of MappedFileStore (requested: $offset + ${src.limit()-src.position()} bytes, available: $size bytes). Please call grow() first." }
        val oldPos = src.position()
        this.fileChannel.write(src, offset)
        src.position(oldPos)
    }

    /**
     * Tries to map the given region of this [Store] and returns a [MappedByteBuffer] for that region. Can be used to ,e.g., map a header of a file directly.
     *
     * @param start The start of the memory region map.
     * @param size The size of the memory region to map.
     * @param mode The [FileChannel.MapMode] to use.
     *
     * @return MappedByteBuffer
     */
    override fun map(start: Long, size: Int, mode: FileChannel.MapMode): MappedByteBuffer = this.fileChannel.map(mode, start, size.toLong())

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
    override fun load() = this.slices.forEach { it.load() }

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
     * @param size The size to which to truncate this [MappedFileChannelStore].
     */
    override fun truncate(size: Long) = this.sliceLock.write {
        /* Check if growth is required at all. */
        if (size > this.size) {
            return
        }

        /* Check if a complete re-mapping of the file is required after truncating the file. */
        val remapRequired = if (size > Integer.MAX_VALUE) {
            30
        } else {
            size.toInt().shift()
        } != this.sliceShift


        try {
            /* Truncate the file. */
            this.fileChannel.truncate(size)

            /* Either re-map or adjust existing mapping. */
            if (remapRequired) {
                this.remap()
                return
            } else {
                /* Determine size of slice array. */
                val slicesOld = this.slices.size
                val slicesNew: Int = (Math.floorDiv(size, this.sliceSize).toInt()) + 1

                /* Unmap existing slices. */
                if (this.forceUnmap) {
                    for (i in slicesNew-1 until slicesOld) {
                        CleanerUtility.forceUnmap(this.slices[i])
                    }
                }
                this.slices = this.slices.copyOfRange(0, slicesNew)

                /* Re-map the last slice. */
                val lastIndex = this.slices.lastIndex
                val chunkSize = if (this.sliceSize * lastIndex + this.sliceSize > size) {
                    this.sliceSize * lastIndex + this.sliceSize - size
                } else {
                    this.sliceSize
                }
                this.slices[lastIndex] = this.fileChannel.map(FileChannel.MapMode.READ_ONLY, this.sliceSize * lastIndex, chunkSize)
            }
        } catch (e: IOException) {
            throw DBException.VolumeIOError(e)
        }
    }

    /**
     * Grows this [MappedFileChannelStore] so as to accommodate at least the given offset in bytes. Calling this method will pre-allocate the required space and update the memory mapping
     *
     * @param newSize The size to which to grow this [MappedFileChannelStore].
     */
    override fun grow(newSize: Long) = this.sliceLock.write {
        /* Check if growth is required at all. */
        if (newSize < this.size) {
            return
        }

        /* Check if a complete re-mapping of the file is required after growing the file. */
        val remapRequired = if (newSize > Integer.MAX_VALUE) {
            30
        } else {
            newSize.toInt().shift()
        } != this.sliceShift

        /* Update the memory mappings. */
        try {
            /* Pre-clear file by appending a EOF byte. */
            this.fileChannel.write(ByteBuffer.allocate(1).put(EOF_BYTE).rewind(),  newSize-1)

            if (remapRequired) {
                this.remap()
                return
            } else {
                val slicesOld = this.slices.size
                val slicesNew: Int = (Math.floorDiv(newSize, this.sliceSize).toInt()) + 1
                if (slicesNew > slicesOld) {
                    val slices2 = (slicesOld until slicesNew).map {
                        val chunkSize = if (this.sliceSize * it + this.sliceSize > newSize) {
                            this.sliceSize * it + this.sliceSize - newSize
                        } else {
                            this.sliceSize
                        }
                        this.fileChannel.map(FileChannel.MapMode.READ_ONLY, this.sliceSize * it, chunkSize)
                    }.toTypedArray()
                    this.slices = (this.slices + slices2)
                }
            }
        } catch (e: IOException) {
            throw StoreException.StoreIOException(this, e)
        }
    }

    /**
     * Closes this [MappedFileChannelStore].
     */
    override fun close() = this.sliceLock.write {
        if (!this.closed.compareAndSet(false, true))
            return

        try {
            /* Release lock and close FileChannel. */
            if (this.fileLock.isValid) {
                this.fileLock.release()
            }
            this.fileChannel.close()

            /* Unmap all the slices. */
            if (this.forceUnmap) {
                for (b in this.slices) {
                    CleanerUtility.forceUnmap(b)
                }
            }

            /* Remove reference to slices! */
            this.slices = emptyArray()
        } catch (e: IOException) {
            throw StoreException.StoreIOException(this, e)
        }
    }

    /**
     * Refreshes the memory mapping of this [MappedFileChannelStore]. This can become necessary, if the file grows significantly.
     * The method determines the size of slice based on the file size. For large files, the slices become larger (up to 4G per slice).
     */
    private fun remap() = this.sliceLock.write {
        try {
            /* Force unmap the open slices (if existing). */
            if (this.forceUnmap) {
                for (slice in slices) {
                    CleanerUtility.forceUnmap(slice)
                }
            }

            /* Do some math regaring size of the slices. */
            val fileSize = this.fileChannel.size()
            this.sliceShift = if (fileSize >= Integer.MAX_VALUE) {
                30
            } else {
                fileSize.toInt().shift()
            }
            this.sliceSize = 1L shl this.sliceShift
            this.sliceSizeModMask = this.sliceSize - 1L

            /* Re-allocate the slices. */
            val chunks = fileSize.roundUp(this.sliceSize).ushr(this.sliceShift)
            this.slices = (0 until chunks).map {
                val chunkSize = if (this.sliceSize * it + this.sliceSize > fileSize) {
                    fileSize - this.sliceSize * it
                } else {
                    this.sliceSize
                }
                this.fileChannel.map(FileChannel.MapMode.READ_ONLY, sliceSize * it, chunkSize)
            }.toTypedArray()
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
        do {
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
     * Returns the slice for the given offset.
     *
     * @param offset The offset (in bytes) into the [MappedFileChannelStore].
     */
    private fun sliceForOffset(offset: Long): ByteBuffer {
        val pos = offset.ushr(this.sliceShift).toInt()
        if (pos > this.slices.lastIndex) {
            throw StoreException.StoreEOFException(this, offset)
        }
        return this.slices[pos]
    }

    /**
     * Returns the slice for the given offset.
     *
     * @param offset The offset (in bytes) into the [MappedFileChannelStore].
     */
    private fun slicesForRange(offset: LongRange): Array<ByteBuffer> {
        val start = offset.first.ushr(this.sliceShift).toInt()
        val end = offset.last.ushr(this.sliceShift).toInt()
        var left = (offset.last - offset.first).toInt()
        if (start > this.slices.lastIndex) {
            throw StoreException.StoreEOFException(this, offset.first)
        }
        if (end > this.slices.lastIndex) {
            throw StoreException.StoreEOFException(this, offset.last)
        }
        return (start..end).map {
            val slice = if (it == start) {
                this.slices[it].duplicate().position((offset.first and this.sliceSizeModMask).toInt()).limit(min(this.slices[it].capacity(), left))
            } else {
                this.slices[it].duplicate().position(0).limit(min(this.slices[it].capacity(), left))
            }
            left -= slice.limit()-slice.position()
            slice
        }.toTypedArray()
    }

    /**
     * Generates a string description of this [MappedFileChannelStore].
     */
    override fun toString(): String = "MappedFileStore{file= ${this.path}, size = ${this.size}}"
}