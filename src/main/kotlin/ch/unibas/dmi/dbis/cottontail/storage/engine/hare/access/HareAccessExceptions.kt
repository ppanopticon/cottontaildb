package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor.TupleId

sealed class HareAccessException(message: String) : Throwable(message)

class TupleIdOutOfBoundException(message: String): HareAccessException(message)

class EntryDeletedException(tupleId: TupleId) : HareAccessException("The entry with the tuple ID $tupleId has been deleted and cannot be accessed.")