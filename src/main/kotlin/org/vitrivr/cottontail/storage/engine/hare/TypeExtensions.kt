package org.vitrivr.cottontail.storage.engine.hare

/**  A [Address], which is a [Long] encoding the [PageId] and [SlotId]. 56 bits are used to encode the [PageId] and 8 bits are used to encode the [SlotId]. */
typealias Address = Long

/** The zero-based ID of a [org.vitrivr.cottontail.storage.engine.hare.basics.Page]. */
typealias PageId = Long

/** The zero-based ID of a slot within a [org.vitrivr.cottontail.storage.engine.hare.basics.Page]. */
typealias SlotId = Short

/** Converts a [PageId] and [SlotId] combination into an [Address]. */
fun addressFor(pageId: PageId, slotId: SlotId) = ((pageId shl 16) or (slotId.toLong() and Short.MAX_VALUE.toLong()))

/** Returns the [PageId] component of the [Address]. */
fun Address.toPageId(): PageId = (this shr 16)

/** Returns the [SlotId] component of the [Address]. */
fun Address.toSlotId(): SlotId = this.toShort()