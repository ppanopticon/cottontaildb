package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor

class EntryDeletedException(tupleId: TupleId) : Throwable("The entry with the tuple ID $tupleId has been deleted and cannot be accessed.")