package org.vitrivr.cottontail.storage.engine.hare

typealias PageId = Long

typealias Address = LongArray

fun Address.slot() = this[0].toInt()

fun Address.page() = this[1]