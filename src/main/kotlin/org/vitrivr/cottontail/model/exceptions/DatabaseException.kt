package org.vitrivr.cottontail.model.exceptions

import org.vitrivr.cottontail.database.column.Type
import org.vitrivr.cottontail.model.basics.Name

open class DatabaseException(message: String) : Throwable(message) {
    /**
     * Thrown when trying to create a [Schema][org.vitrivr.cottontail.database.schema.Schema]
     * that does already exist.
     *
     * @param schema [Name] of the [Schema][org.vitrivr.cottontail.database.schema.Schema].
     */
    class SchemaAlreadyExistsException(schema: Name) : DatabaseException("Schema '$schema' does already exist!")

    /**
     * Thrown when trying to access a [Schema][org.vitrivr.cottontail.database.schema.Schema]
     * that does not exist.
     *
     * @param schema [Name] of the [Schema][org.vitrivr.cottontail.database.schema.Schema].
     */
    class SchemaDoesNotExistException(val schema: Name.SchemaName) : DatabaseException("Schema '$schema' does not exist!")

    /**
     * Thrown when trying to create an [Entity][org.vitrivr.cottontail.database.entity.Entity]
     * that does already exist.
     *
     * @param entity [Name] of the [Entity][org.vitrivr.cottontail.database.entity.Entity].
     */
    class EntityAlreadyExistsException(entity: Name.EntityName) : DatabaseException("Entity '$entity' does already exist!")

    /**
     * Thrown when trying to access an [Entity][org.vitrivr.cottontail.database.entity.Entity]
     * that does not exist.
     *
     * @param entity [Name] of the [Entity][org.vitrivr.cottontail.database.entity.Entity].
     */
    class EntityDoesNotExistException(val entity: Name.EntityName) : DatabaseException("Entity '$entity' does not exist!")

    /**
     * Thrown whenever trying to create an [Index][org.vitrivr.cottontail.database.index.Index]
     * that does already exist.
     *
     * @param index The [Name] of the [Index][org.vitrivr.cottontail.database.index.Index]
     */
    class IndexAlreadyExistsException(val index: Name.IndexName) : DatabaseException("Index '$index' does already exist!")

    /**
     * Thrown whenever trying to access an [Index][org.vitrivr.cottontail.database.index.Index]
     * that does not exist.
     *
     * @param index The [Name] of the [Index][org.vitrivr.cottontail.database.index.Index]
     */
    class IndexDoesNotExistException(val index: Name) : DatabaseException("Index '$index' does not exist!")

    /**
     * Thrown whenever trying to create an [Index][[org.vitrivr.cottontail.database.index.Index] that is not supported (yet). *
     *
     * @param index The [Name] of the [Index][org.vitrivr.cottontail.database.index.Index]
     */
    class IndexNotSupportedException(val index: Name.IndexName, val reason: String) : DatabaseException("Index '$index' could not be created: $reason")

    /**
     * Thrown upon creation of an [Entity][org.vitrivr.cottontail.database.entity.Entity] if a duplicate column name is encountered.
     *
     * @param column [Name.ColumnName] of the [Column][org.vitrivr.cottontail.database.column.Column]s in the definition.
     */
    class DuplicateColumnException(column: Name.ColumnName) : DatabaseException("Column $column could not be created because a duplicate entry was encountered!")

    /**
     * Thrown whenever trying to access a [Column][org.vitrivr.cottontail.database.column.Column]
     * that does not exist.
     *
     * @param column The [Name] of the [Column][org.vitrivr.cottontail.database.column.Column].
     */
    class ColumnDoesNotExistException(val column: Name.ColumnName) : DatabaseException("Column $column does not exist.")

    /**
     * Thrown by [Index][org.vitrivr.cottontail.database.index.Index]es if they are given a
     * [Predicate][org.vitrivr.cottontail.database.queries.Predicate] they cannot executed.
     *
     * @param index [Name] of the affected [Index][org.vitrivr.cottontail.database.index.Index]
     */
    class PredicateNotSupportedBxIndexException(index: Name) : DatabaseException("Index '$index' cannot be used to execute the given predicate.")

    /**
     *
     */
    class ColumnTypeUnexpectedException(column: Name, expected: Type<*>, actual: Type<*>) :
        DatabaseException("Column '$column' has wrong type (expected: ${expected.name}, actual: ${actual.name}).")

    /**
     * Thrown when the Cottontail DB engine expects a different type of file.
     *
     * @param type The name of the expected type.
     */
    class InvalidFileException(type: String) : DatabaseException("The provided file is not a valid $type file!")

    /**
     * Thrown when the Cottontail DB engine cannot read the data from a file OR the data read is unexpected. Usually, this can be attributed to data corruption
     *
     * @param message Description of the issue.
     */
    class DataCorruptionException(message: String) : DatabaseException(message)
}




