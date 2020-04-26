package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.memory

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.ReferenceCounted
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DataPage
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.convertWriteLock
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

import java.nio.ByteBuffer

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.StampedLock

/**
 *
 */
object MemoryManager: AutoCloseable {

    val USE_DIRECT_MEMORY = System.getProperty("cottontail.hare.buffer.direct","false")!!.toBoolean()
    val MAXIMUM_DIRECT_MEMORY = System.getProperty("cottontail.hare.buffer.limit", "256").toInt() * 100_000L

    /** Size of the memory allocated by this [MemoryManager]. */
    @Volatile
    var allocatedBytes = 0L
        private set

    /** Number of bytes that are available for this [MemoryManager]. */
    val availableBytes
        get() = if (USE_DIRECT_MEMORY) {
            MAXIMUM_DIRECT_MEMORY
        } else {
            (Runtime.getRuntime().maxMemory() / 2)
        }

    /** Array of [DataPage]s that are kept in memory. */
    private val unusedBlocks = mutableSetOf<MemoryBlock>()

    /** An internal lock to mediate access to allocation. */
    private val allocationLock = StampedLock()

    /** Cleaner operation that removes allocated MemoryBlocks */
    private val cleaner = GlobalScope.launch {
        while (true) {
            if (sweep() == 0L) {
                delay(500)
            }
        }
    }

    /**
     * Allocates and returns a new [MemoryBlock]
     *
     * @param size Size of the [MemoryBlock].
     * @return Allocated [MemoryBlock].
     */
    fun request(size: Int): MemoryBlock {
        var stamp = this.allocationLock.readLock()
        try {
            val candidate = this.unusedBlocks.firstOrNull { it.size == size }
            if (candidate != null) {
                return candidate.retain()
            }

            /* Convert to write lock. */
            stamp = this.allocationLock.convertWriteLock(stamp)
            return MemoryBlock(size)
        } finally {
            this.allocationLock.unlock(stamp)
        }
    }

    /**
     * Sweeps through [unusedBlocks] and deallocates them if [allocatedBytes] greater than [maximumBytes].
     *
     * @return Number of bytes that were freed.
     */
    fun sweep(): Long {
        var deallocated = 0L
        while (this.allocatedBytes > this.availableBytes) {
            val block = this.unusedBlocks.firstOrNull()
            if (block != null) {
                block.deallocate()
                deallocated += block.size
            }
        }
        if (deallocated > 0) {
            System.gc() /* Run garbage collection. */
        }
        return deallocated
    }

    /**
     * A contiguous junk of memory wrapped by a [ByteBuffer].
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    class MemoryBlock(val size: Int) : ReferenceCounted {

        private val _refCount = AtomicInteger(1)
        override val refCount: Int
            get() = _refCount.get()

        init {
            allocatedBytes += this.size
        }

        /** The [ByteBuffer] backing this [MemoryBlock]. */
        private val buffer: ByteBuffer = if (USE_DIRECT_MEMORY) {
            ByteBuffer.allocateDirect(this.size)
        } else {
            ByteBuffer.allocate(this.size)
        }

        /** */
        fun paged(pageSlice: Int, pages: Int) = Array<ByteBuffer>(pages) {
            this.buffer.position(it shl pageSlice).limit((it+1) shl pageSlice).slice()
        }

        /**
         * Retains this [MemoryBlock], increasing its [refCount] by 1.
         *
         * @return This [MemoryBlock]
         */
        override fun retain(): MemoryBlock {
            val old = this._refCount.getAndUpdate {
                check(it != ReferenceCounted.REF_COUNT_DISPOSED) { "PageRef $this has been disposed and cannot be accessed anymore." }
                it + 1
            }
            if (old == 0) {
                unusedBlocks.remove(this)
            }
            return this
        }

        /**
         * Releases this [MemoryBlock], decreasing its [refCount] by 1. Once the [refCount] drops to
         * zero, the block may be deallocated anytime.
         *
         * @return This [MemoryBlock]
         */
        override fun release() {
            val new = this._refCount.updateAndGet {
                check(it != ReferenceCounted.REF_COUNT_DISPOSED) { "PageRef $this has been disposed and cannot be accessed anymore." }
                check(it > 0) { "MemoryBlock $this has a reference count of zero and cannot be released!" }
                it - 1
            }
            if (new == 0) {
                unusedBlocks.add(this)
            }
        }

        /**
         * Explicitly de-allocates this [MemoryBlock]. Usually, this method is called by the [MemoryManager]. However,
         * a caller may use method to explicitly dispose of a [MemoryBlock]. Calling this method wile [refCount] is
         * greater than zero, is considered a programmer's error and an [IllegalStateException] will be thrown.
         */
        fun deallocate() {
            if (this._refCount.compareAndSet(0, ReferenceCounted.REF_COUNT_DISPOSED)) {
                allocationLock.write {
                    unusedBlocks.remove(this)
                    allocatedBytes -= this.size
                }
            } else {
                throw IllegalStateException("This MemoryBlock is still retained by some callsite and cannot be deallocated.")
            }
        }
    }

    /**
     *
     */
    override fun close() {
    }
}