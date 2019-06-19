package ch.unibas.dmi.dbis.cottontail.model.exceptions

import ch.unibas.dmi.dbis.cottontail.model.type.Type
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name

open class DatabaseException(message: String) : Throwable(message) {
    /**
     * Thrown when trying to create a [Schema] that does already exist.
     *
     * @param entity [Name] of the [Schema].
     */
    class SchemaAlreadyExistsException(schema: Name): DatabaseException("Schema '$schema' does already exist!")

    /**
     * Thrown when trying to access a [Schema] that does not exist.
     *
     * @param entity [Name] of the [Schema].
     */
    class SchemaDoesNotExistException(schema: Name): DatabaseException("Schema '$schema' does not exist!")

    /**
     * Thrown when trying to create an [Entity] that does already exist.
     *
     * @param entity [Name] of the [Entity].
     */
    class EntityAlreadyExistsException(entity: Name): DatabaseException("Entity '$entity' does already exist!")

    /**
     * Thrown when trying to access an [Entity] that does not exist.
     *
     * @param entity [Name] of the [Entity].
     */
    class EntityDoesNotExistException(entity: Name): DatabaseException("Entity '$entity' does not exist!")

    /**
     * Thrown whenever trying to create an [Index] that does already exist.
     *
     * @param index The [Name] of the [Index]
     */
    class IndexAlreadyExistsException(val index: String): DatabaseException("Index '$index' does already exist!")

    /**
     * Thrown whenever trying to access an [Index] that does not exist.
     *
     * @param index The [Name] of the [Index]
     */
    class IndexDoesNotExistException(val index: String): DatabaseException("Index '$index' does not exist!")

    /**
     * Thrown upon creation of an [Entity] if the definition contains duplicate column names.
     *
     * @param fqn [Name] of the affected [Entity]
     * @param columns [Name] of the [Column]s in the definition.
     */
    class DuplicateColumnException(entity: Name, columns: Collection<Name>) : DatabaseException("Entity '$entity' could not be created because it contains duplicate column names (c=[${columns.joinToString(",")}])!")

    /**
     * Thrown whenever trying to access a [Column] that does not exist.
     *
     * @param column The [Name] of the [Column].
     */
    class ColumnDoesNotExistException(val column: Name): DatabaseException("Column $column does not exist.")

    /**
     * Thrown by [Index]es if they are given a [Predicate] they cannot executed.
     *
     * @param index [Name] of the affected [Index]
     */
    class PredicateNotSupportedBxIndexException(index: Name): DatabaseException("Index '$index' cannot be used to execute the given predicate.")

    /**
     *
     */
    class ColumnTypeUnexpectedException(column: Name, expected: Type<*>, actual: Type<*>): DatabaseException("Column '$column' has wrong type (expected: ${expected.name}, actual: ${actual.name}).")

    /**
     * Thrown when the Cottontail DB engine expects a different type of file.
     *
     * @param type The name of the expected type.
     */
    class InvalidFileException(type: String): DatabaseException("The provided file is not a valid $type file!")

    /**
     * Thrown when the Cottontail DB engine cannot read the data from a file OR the data read is unexpected. Usually, this can be attributed to data corruption
     *
     * @param message Description of the issue.
     */
    class DataCorruptionException(message: String): DatabaseException(message)
}




