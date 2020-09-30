package org.mapdb

import org.mapdb.volume.Volume
import org.mapdb.volume.VolumeFactory
import org.vitrivr.cottontail.storage.store.CleanerUtility
import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.max
import kotlin.math.min


/**
 * This is a re-implementation / copy of Map DB's [ByteBufferVol][org.mapdb.volume.ByteBufferVol] class with minor modifications.
 *
 * @version 1.0
 * @author Ralph Gasser
 */
class CottontailDBVolume (val path: Path, val sliceShift: Int, val readonly: Boolean = false, val forceUnmap: Boolean = false, val initSize: Long = 2048L, val lockTimeout: Long = 5000): Volume() {

    companion object {
        const val WRITE_SIZE = 1024
        fun toByte(byt: Int): Byte = (byt and 0xff).toByte()
    }

    /**
     * Factory class.
     */
    class CottontailDBVolumeFactory(val forceUnmap: Boolean) : VolumeFactory() {

        override fun makeVolume(file: String, readOnly: Boolean, fileLockWait: Long, sliceShift: Int, initSize: Long, fixedSize: Boolean): Volume {
            return factory(file, readOnly, fileLockWait, sliceShift, this.forceUnmap, initSize)
        }

        override fun exists(file: String?): Boolean {
            return File(file!!).exists()
        }

        override fun handlesReadonly(): Boolean {
            return true
        }

        private fun factory(file: String, readOnly: Boolean, fileLockWait: Long, sliceShift: Int, forceUnmap: Boolean, initSize: Long): Volume {
            val path = Paths.get(file)
            return CottontailDBVolume(path = path, sliceShift = sliceShift, readonly = readOnly, forceUnmap = forceUnmap, initSize = initSize, lockTimeout = fileLockWait)
        }
    }

    /** The [FileChannel] used for data access. */
    private val fileChannel = if (this.readonly) {
        FileChannel.open(this.path, StandardOpenOption.READ)
    } else {
        FileChannel.open(this.path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.SPARSE, StandardOpenOption.CREATE)
    }

    /** Acquires a [FileLock] for file underlying this [FileChannel]. */
    private var fileLock = this.acquireFileLock(5000L)

    /** The size of an individual slice (in bytes). */
    private val sliceSize = 1L shl sliceShift

    /** Mask that can be used to identify the the slice part of the offset. */
    private val sliceSizeModMask = sliceSize - 1L

    /** Internal lock used to mediate access to changes to the slicing. */
    private val growLock = ReentrantLock()

    @Volatile
    private var slices = emptyArray<ByteBuffer?>()

    init {
        try {
            val mapMode = if (this.readonly) FileChannel.MapMode.READ_ONLY else FileChannel.MapMode.READ_WRITE
            val fileSize = this.fileChannel.size()
            var endSize = fileSize
            if (initSize > fileSize && !this.readonly)
                endSize = initSize //allocate more data

            if (endSize > 0) {
                val chunksSize = DataIO.roundUp(endSize, sliceSize).ushr(sliceShift).toInt()
                if (endSize > fileSize && !this.readonly) {
                    this.fileChannel.write(ByteBuffer.allocate(1), endSize-1)
                }

                this.slices = arrayOfNulls(chunksSize)
                for (pos in this.slices.indices) {
                    val b = this.fileChannel.map(mapMode, 1L * sliceSize * pos.toLong(), sliceSize)
                    assert(b.order() == ByteOrder.BIG_ENDIAN) { "Data must be in Big Endian order!" }
                    this.slices[pos] = b
                }
            }
        } catch (e: IOException) {
            throw DBException.VolumeIOError(e)
        }
    }


    /**
     * Forces the content of the file underpinning this [CottontailDBVolume] into physical memory using a best effort attempt.
     *
     * @return True on success.
     */
    override fun fileLoad(): Boolean {
        val slices = this.slices
        for (b in slices) {
            if (b is MappedByteBuffer) {
                b.load()
            }
        }
        return true
    }



    override fun truncate(size: Long) {
        val maxSize = 1 + size.ushr(this.sliceShift).toInt()
        val mapMode = if (this.readonly) FileChannel.MapMode.READ_ONLY else FileChannel.MapMode.READ_WRITE
        if (maxSize == this.slices.size)
            return

        if (maxSize > this.slices.size) {
            ensureAvailable(size)
            return
        }

        this.growLock.withLock {
            try {
                if (maxSize >= slices.size)
                    return
                val old = slices
                slices = Arrays.copyOf<ByteBuffer>(slices, maxSize)

                /* Force unmap the remaining buffers. */
                if (this.forceUnmap) {
                    for (i in maxSize until old.size) {
                        val b = old[i]
                        if (b is MappedByteBuffer) {
                            CleanerUtility.forceUnmap(b)
                        }
                        old[i] = null
                    }
                }

                /* Truncate the file. */
                this.fileChannel.truncate(1L *  this.sliceSize.toLong() * maxSize.toLong())
            } catch (e: IOException) {
                throw DBException.VolumeIOError(e)
            }
        }
    }

    /**
     * Forces all changes made to this [CottontailDBVolume] to disk. If this [CottontailDBVolume] was opened in
     * read-only mode, then invoking this method has no effect.
     */
    override fun sync() {
        if (this.readonly)
            return

        this.growLock.withLock {
            val slices = this.slices
            for (i in slices.indices.reversed()) {
                val b = slices[i]
                if (b is MappedByteBuffer) {
                    b.force()
                }
            }
        }
    }

    /**
     *
     */
    override fun clear(startOffset: Long, endOffset: Long) {
        assert(startOffset <= endOffset) { "Start of the offset must me smaller or equal than the end of the offset."}
        var remaining = endOffset - startOffset
        var written = 0L
        val startSlice = startOffset.ushr(this.sliceShift).toInt()
        val endSlice = endOffset.ushr(this.sliceShift).toInt()

        for (slice in startSlice until endSlice) {
            var buf = this.getSlice(startOffset)
            val start = if (slice == startSlice) {
                ((startOffset and sliceSizeModMask) + written).toInt()
            } else {
                0
            }
            val end = max(start + remaining, this.sliceSize).toInt()

            var pos = start
            while (pos < end) {
                val write = min(WRITE_SIZE, end - start)
                buf = buf.duplicate()
                buf.position(pos)
                buf.put(ByteArray(WRITE_SIZE), 0, write)
                pos += write
                remaining -= write
                written += write
            }
        }
    }

    /**
     * Ensures that the slice referenced by a given offset is available and creates the portion of the file, if necessary.
     *
     * @param offset The offset (in bytes) into the file.
     */
    override fun ensureAvailable(offset: Long) {
        val endOffset = DataIO.roundUp(offset, 1L shl sliceShift)
        val slicePos = endOffset.ushr(sliceShift).toInt()
        val mapMode = if (this.readonly) FileChannel.MapMode.READ_ONLY else FileChannel.MapMode.READ_WRITE
        if (slicePos < this.slices.size) {
            return
        }

        this.growLock.withLock {
            try {
                /* Check second time (with lock). */
                if (slicePos <= this.slices.size)
                    return

                val oldSize = this.slices.size

                /* Pre-clear of appended file. */
                this.fileChannel.write(ByteBuffer.allocate(1), endOffset-1)

                /* Grow slices. */
                var slices2 = this.slices
                slices2 = slices2.copyOf(slicePos)

                for (pos in oldSize until slices2.size) {
                    val b = this.fileChannel.map(mapMode, 1L * sliceSize * pos.toLong(), sliceSize)
                    assert(b.order() == ByteOrder.BIG_ENDIAN) { "Data must be in Big Endian order!" }
                    slices2[pos] = b
                }

                this.slices = slices2
            } catch (e: IOException) {
                throw DBException.VolumeIOError(e)
            }
        }
    }

    /**
     * Closes this [CottontailDBVolume] and the underlying [FileChannel]. All the referenced [ByteBuffer]s will be
     * released when invoking this method.
     *
     * <strong>IMPORTANT:<strong> Due to a peculiarity of how memory mapped files are handled by the JVM, calling this
     * method does NOT unmap the memory mapped regions until the [ByteBuffer]s are actually garbage collected. Set the
     * [CottontailDBVolume.forceUnmap] option to true, to enforce unmapping.
     */
    override fun close() {
        if (!this.closed.compareAndSet(false, true))
            return

        this.growLock.withLock {
            try {
                /* Release lock and close FileChannel. */
                if (this.fileLock.isValid) {
                    this.fileLock.release()
                }
                this.fileChannel.close()

                /* Unmap all the slices. */
                if (this.forceUnmap) {
                    for (b in this.slices) {
                        if (b is MappedByteBuffer) {
                            CleanerUtility.forceUnmap(b)
                        }
                    }
                }

                /* Null al the slices. */
                this.slices.fill(null, 0, this.slices.size)
                this.slices = emptyArray()
            } catch (e: IOException) {
                throw DBException.VolumeIOError(e)
            }
        }
    }

    override fun getByte(offset: Long): Byte = this.getSlice(offset)[(offset and sliceSizeModMask).toInt()]

    override fun putByte(offset: Long, value: Byte) {
        this.getSlice(offset).put((offset and this.sliceSizeModMask).toInt(), value)
    }

    override fun getInt(offset: Long): Int = this.getSlice(offset).getInt((offset and sliceSizeModMask).toInt())

    override fun putInt(offset: Long, value: Int) {
        this.getSlice(offset).putInt((offset and this.sliceSizeModMask).toInt(), value)
    }

    override fun getLong(offset: Long): Long = this.getSlice(offset).getLong((offset and sliceSizeModMask).toInt())

    override fun putLong(offset: Long, value: Long){
        this.getSlice(offset).putLong((offset and this.sliceSizeModMask).toInt(), value)
    }

    override fun getUnsignedShort(offset: Long): Int {
        val b = getSlice(offset)
        var bpos = (offset and sliceSizeModMask).toInt()
        return b[bpos++].toInt() and 0xff shl 8 or (b[bpos].toInt() and 0xff)
    }

    override fun putUnsignedShort(offset: Long, value: Int) {
        val b = this.getSlice(offset)
        var bpos = (offset and sliceSizeModMask).toInt()

        b.put(bpos++, (value shr 8).toByte())
        b.put(bpos, value.toByte())
    }
    override fun getUnsignedByte(offset: Long): Int {
        val b = getSlice(offset)
        val bpos = (offset and sliceSizeModMask).toInt()
        return (b[bpos].toInt() and 0xff)
    }

    override fun putUnsignedByte(offset: Long, byt: Int) {
        val b = getSlice(offset)
        val bpos = (offset and sliceSizeModMask).toInt()

        b.put(bpos, toByte(byt))
    }

    override fun getSixLong(pos: Long): Long {
        val bb = getSlice(pos)
        var bpos = (pos and sliceSizeModMask).toInt()

        return (bb[bpos++].toInt() and 0xff).toLong() shl 40 or
                ((bb[bpos++].toInt() and 0xff).toLong() shl 32) or
                ((bb[bpos++].toInt() and 0xff).toLong() shl 24) or
                ((bb[bpos++].toInt() and 0xff).toLong() shl 16) or
                ((bb[bpos++].toInt() and 0xff).toLong() shl 8) or
                (bb[bpos].toInt() and 0xff).toLong()
    }

    override fun putSixLong(pos: Long, value: Long) {
        val b = getSlice(pos)
        val f = 0xff.toLong()
        var bpos = (pos and sliceSizeModMask).toInt()

        if (CC.ASSERT && value.ushr(48) != 0L)
            throw DBException.DataCorruption("six long out of range")

        b.put(bpos++, (f and (value shr 40)).toByte())
        b.put(bpos++, (f and (value shr 32)).toByte())
        b.put(bpos++, (f and (value shr 24)).toByte())
        b.put(bpos++, (f and (value shr 16)).toByte())
        b.put(bpos++, (f and (value shr 8)).toByte())
        b.put(bpos, (f and value).toByte())
    }

    override fun putPackedLong(pos: Long, value: Long): Int {
        val b = getSlice(pos)
        val bpos = (pos and sliceSizeModMask).toInt()

        //$DELAY$
        var ret = 0
        var shift = 63 - java.lang.Long.numberOfLeadingZeros(value)
        shift -= shift % 7 // round down to nearest multiple of 7
        while (shift != 0) {
            b.put(bpos + ret++, (value.ushr(shift) and 0x7F).toByte())
            //$DELAY$
            shift -= 7
        }
        b.put(bpos + ret++, (value and 0x7F or 0x80).toByte())
        return ret
    }

    override fun getPackedLong(position: Long): Long {
        val b = getSlice(position)
        val bpos = (position and sliceSizeModMask).toInt()

        var ret: Long = 0
        var pos2 = 0
        var v: Int
        do {
            v = b.get(bpos + pos2++).toInt()
            ret = ret shl 7 or (v and 0x7F).toLong()
        } while (v and 0x80 == 0)

        return pos2.toLong() shl 60 or ret
    }

    override fun getData(offset: Long, bytes: kotlin.ByteArray?, bytesPos: Int, size: Int) {
        val b1 = getSlice(offset).duplicate()
        val bufPos = (offset and sliceSizeModMask).toInt()

        b1.position(bufPos)
        b1.get(bytes, bytesPos, size)
    }

    override fun putData(offset: Long, src: kotlin.ByteArray, srcPos: Int, srcSize: Int) {
        val b1 = getSlice(offset).duplicate()
        val bufPos = (offset and sliceSizeModMask).toInt()

        b1.position(bufPos)
        b1.put(src, srcPos, srcSize)
    }

    override fun putData(offset: Long, buf: ByteBuffer?) {
        val b1 = getSlice(offset).duplicate()
        val bufPos = (offset and sliceSizeModMask).toInt()
        //no overlap, so just write the value
        b1.position(bufPos)
        b1.put(buf)
    }

    override fun putDataOverlap(offset: Long, data: ByteArray, pos: Int, len: Int) {
        var offset = offset
        var pos = pos
        var len = len
        val overlap = offset.ushr(sliceShift) != (offset + len).ushr(sliceShift)

        if (overlap) {
            while (len > 0) {
                val b = getSlice(offset).duplicate()
                b.position((offset and sliceSizeModMask).toInt())

                val toPut = min(len, (sliceSize - b.position()).toInt())

                b.limit(b.position() + toPut)
                b.put(data, pos, toPut)

                pos += toPut
                len -= toPut
                offset += toPut.toLong()
            }
        } else {
            putData(offset, data, pos, len)
        }
    }

    override fun getDataInputOverlap(offset: Long, size: Int): DataInput2 {
        var offset = offset
        var size = size
        val overlap = offset.ushr(sliceShift) != (offset + size).ushr(sliceShift)
        if (overlap) {
            val bb = ByteArray(size)
            val origLen = size
            while (size > 0) {
                val b = getSlice(offset).duplicate()
                b.position((offset and sliceSizeModMask).toInt())

                val toPut = min(size, (sliceSize - b.position()).toInt())

                b.limit(b.position() + toPut)
                b.get(bb, origLen - size, toPut)
                size -= toPut
                offset += toPut.toLong()
            }
            return DataInput2.ByteArray(bb)
        } else {
            //return mapped buffer
            return getDataInput(offset, size)
        }
    }

    override fun copyTo(inputOffset: Long, target: Volume, targetOffset: Long, size: Long) {
        val b1 = getSlice(inputOffset).duplicate()
        val bufPos = (inputOffset and sliceSizeModMask).toInt()

        b1.position(bufPos)
        b1.limit((bufPos + size).toInt())
        target.putData(targetOffset, b1)
    }

    /**
     *
     */
    override fun getDataInput(offset: Long, size: Int): DataInput2 = DataInput2.ByteBuffer(getSlice(offset), (offset and sliceSizeModMask).toInt())

    /**
     * Returns a [File] object for the file underpinning this [CottontailDBVolume].
     *
     * @return [File] object
     */
    override fun getFile(): File = this.path.toFile()

    /**
     * Returns the size of the file underpinning this [CottontailDBVolume].
     *
     * @return File size.
     */
    override fun length(): Long = this.fileChannel.size()

    /**
     * Returns true if this [CottontailDBVolume]was opened in read-only mode and false otherwise.
     *
     * @return Read-only mode of this [CottontailDBVolume].
     */
    override fun isReadOnly(): Boolean = this.readonly

    /**
     * Returns true if the [FileLock] underpinning this [FileChannel] is still valid.
     *
     * @return True if file underpinning this [FileChannel] is locked, false otherwise.
     */
    override fun getFileLocked(): Boolean = this.fileLock.isValid

    /**
     * Always returns true.
     *
     * @return true
     */
    override fun isSliced(): Boolean = true

    /**
     * Returns the size (in bytes) of an individual slice managed by this [CottontailDBVolume].
     *
     * @return Size of a slice.
     */
    override fun sliceSize(): Int = this.sliceSize.toInt()

    /**
     * Returns the slice for the given offset.
     *
     * @param offset The offset (in bytes) into the [CottontailDBVolume].
     */
    fun getSlice(offset: Long): ByteBuffer {
        val slices = this.slices
        val pos = offset.ushr(this.sliceShift).toInt()
        return slices[pos] ?: throw DBException.VolumeEOF("Get/Set beyond file size. Requested offset: " + offset + ", volume size: " + length())
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
                    Thread.sleep(250)
                }
            } catch (e: IOException) {
                throw DBException.VolumeIOError(e)
            }
        } while (System.currentTimeMillis() - start < timeout)
        throw DBException.FileLocked(this.path, null)
    }
}
