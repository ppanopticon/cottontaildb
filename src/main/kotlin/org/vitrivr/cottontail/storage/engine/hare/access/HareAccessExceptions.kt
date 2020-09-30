package org.vitrivr.cottontail.storage.engine.hare.access

sealed class HareAccessException(message: String) : Throwable(message)

/**
 * Thrown when trying to access an [TupleId] that does not exist.
 */
class TupleIdOutOfBoundException(message: String): HareAccessException(message)

/**
 * Thrown when trying to access an entry that has been deleted.
 */
class EntryDeletedException(message: String) : HareAccessException(message)

/**
 * Thrown when a null value is written to a HARE data structure, that does not support null values.
 */
class NullValueNotAllowedException(message: String): HareAccessException(message)