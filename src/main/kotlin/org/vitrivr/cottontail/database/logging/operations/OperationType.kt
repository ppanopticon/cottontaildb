package org.vitrivr.cottontail.database.logging.operations

/**
 * Enumeration of all [Operation]s supported by the Cottontail DB database engine.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class OperationType(val finalize: Boolean) {
    INSERT(false),
    UPDATE(false),
    DELETE(false),
    CREATE_SCHEMA(false),
    DROP_SCHEMA(false),
    CREATE_ENTITY(false),
    DROP_ENTITY(false),
    CREATE_INDEX(false),
    DROP_INDEX(false),
    COMMIT(true),
    ROLLBACK(true)
}