package org.vitrivr.cottontail.storage.store

import org.slf4j.LoggerFactory

import java.lang.reflect.Method
import java.nio.ByteBuffer
import java.nio.MappedByteBuffer

/**
 * A helper class that facilitates unmapping of [MappedByteBuffer].
 *
 * @see MappedByteBuffer
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object CleanerUtility {

    /** Flag indicating whether the current Java version is < or >= 9. */
    private val PRE_JAVA_9 = System.getProperty("java.specification.version", "9").startsWith("1.")

    /** Reference to the cleaner method (generated once). */
    private var CLEANER_METHOD: Method? = null

    /** Reference to the Unsafe instance (generated once); only required for Java 9+- */
    private var UNSAFE_INSTANCE: Any? = null

    /** Logger instance. */
    private val LOGGER = LoggerFactory.getLogger(CleanerUtility::class.java)

    init {
        try {
            if (PRE_JAVA_9) {
                val cleanMethod = Class.forName("sun.misc.Cleaner").getMethod("clean")
                CLEANER_METHOD = cleanMethod
            } else {
                val unsafeClass = try {
                    Class.forName("sun.misc.Unsafe")
                } catch (e: Exception) {
                    Class.forName("jdk.internal.misc.Unsafe")
                }
                CLEANER_METHOD = unsafeClass.getMethod("invokeCleaner", ByteBuffer::class.java)
                val field = unsafeClass.getDeclaredField("theUnsafe")
                field.isAccessible = true
                UNSAFE_INSTANCE = field.get(null)
            }
            CLEANER_METHOD?.isAccessible = true
        } catch (ex: Exception) {
            if (PRE_JAVA_9) {
                LOGGER.warn("Could not obtain a valid instance of sun.misc.Cleaner. Therefore, memory-mapped files will probably not be unmapped until garbage collected.")
            } else {
                LOGGER.warn("Could not obtain a valid instance of sun.misc.Unsafe. Therefore, memory-mapped files will probably not be unmapped until garbage collected.")
            }
        }
    }

    /**
     * Force-unmaps a [MappedByteBuffer]. The [MappedByteBuffer] must not be used after invoking this method!
     *
     * @param buffer [MappedByteBuffer] to unmap.
     */
    fun forceUnmap(buffer: MappedByteBuffer) = if (PRE_JAVA_9) {
        CLEANER_METHOD?.invoke(null, buffer)
    } else {
        CLEANER_METHOD?.invoke(UNSAFE_INSTANCE, buffer)
    }
}