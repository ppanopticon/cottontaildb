package org.vitrivr.cottontail.storage.engine.hare.views

/** Mask used to determine, whether an entry has been deleted. */
const val VARIABLE_FLAGS_MASK_INITIALIZED = 1 shl 0

/** Mask used to determine, whether an entry has been deleted. */
const val VARIABLE_FLAGS_MASK_DELETED = 1 shl 1

/** Mask used to determine, whether an entry is null. */
const val VARIABLE_FLAGS_MASK_NULL = 1 shl 2

/** A [Int] used to encode [Flags]. */
typealias Flags = Int

/**
 * Returns true, if the [Flags] indicate that the entry has been deleted.
 *
 * @return true, if the deleted flag has been set, false otherwise.
 */
fun Flags.isInitialized(): Boolean = (this and VARIABLE_FLAGS_MASK_INITIALIZED) > 0L

/**
 * Returns true, if the [Flags] indicate that the entry has been deleted.
 *
 * @return true, if the deleted flag has been set, false otherwise.
 */
fun Flags.isDeleted(): Boolean = (this and VARIABLE_FLAGS_MASK_DELETED) > 0L

/**
 * Returns true, if the [Flags] indicate that the entry is null.
 *
 * @return true, if the null flag has been set, false otherwise.
 */
fun Flags.isNull(): Boolean = (this and VARIABLE_FLAGS_MASK_NULL) > 0L


