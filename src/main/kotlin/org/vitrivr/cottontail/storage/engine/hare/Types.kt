package org.vitrivr.cottontail.storage.engine.hare

import org.vitrivr.cottontail.model.basics.TupleId


/** Address of an entry as [Long]. 56 bits are used to encode the [PageId] and 8 bits are used to encode the [SlotId]. */
typealias Address = Long

/** The zero-based ID of a [org.vitrivr.cottontail.storage.engine.hare.basics.Page]. */
typealias PageId = Long

/** The zero-based ID of a slot within a [org.vitrivr.cottontail.storage.engine.hare.basics.Page]. */
typealias SlotId = Short

/** */
fun TupleId.toAddress(slots: Int) = (((this / slots) + 1L) shl 16) or ((this % slots) and Short.MAX_VALUE.toLong())

/** */
fun Address.pageId(): PageId = (this shr 16)

/** */
fun Address.slot(): SlotId = this.toShort()

/** Converts a [SlotId] to an offset into a [org.vitrivr.cottontail.storage.engine.hare.basics.Page]. */
fun SlotId.toHeaderOffset(): Int = (this.toInt() shl 2)