package org.vitrivr.cottontail.utilities.extensions

import java.nio.ByteBuffer
import java.nio.CharBuffer

/**
 * Writes an UTF-8 [String] to this [ByteBuffer].
 *
 * @param str [String] to write.
 * @return This [ByteBuffer]
 */
fun ByteBuffer.putString(str: String): ByteBuffer {
    this.putInt(str.length)
    for (c in str) {
        this.putChar(c)
    }
    return this
}


/**
 * Writes an UTF-8 [String] to this [ByteBuffer] at the given position.
 *
 * @param index The index to write to.
 * @param str [String] to write.
 * @return This [ByteBuffer]
 */
fun ByteBuffer.putString(index: Int, str: String): ByteBuffer {
    this.mark()
    this.position(index)
    this.putString(str)
    return this.rewind()
}

/**
 * Writes an UTF-8 [String] to this [ByteBuffer].
 *
 * @return [String]
 */
fun ByteBuffer.getString(): String {
    val buffer = CharBuffer.allocate(this.int)
    for (i in 0 until buffer.capacity()) {
        buffer.put(this.char)
    }
    return buffer.toString()
}


/**
 * Writes an UTF-8 [String] to this [ByteBuffer] at the given position.
 *
 * @return [String]
 */
fun ByteBuffer.getString(index: Int): String {
    this.mark()
    this.position(index)
    val ret = this.getString()
    this.rewind()
    return ret
}