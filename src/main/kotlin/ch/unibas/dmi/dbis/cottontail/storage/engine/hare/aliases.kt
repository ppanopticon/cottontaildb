package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk

/** ID of a [Page]. */
typealias PageId = Long

/** Reference to a [Page]. Consists of a Index to the buffer pool, a access counter and a dirty flag. */
typealias PageRef = Triple<Int,Int,Boolean>