package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.access.cursor

import ch.unibas.dmi.dbis.cottontail.model.values.types.Value

interface Cursor<T: Value> : ReadableCursor<T>, WritableCursor<T>