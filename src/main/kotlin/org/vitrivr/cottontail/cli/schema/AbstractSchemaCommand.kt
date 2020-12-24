package org.vitrivr.cottontail.cli.schema

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.model.basics.Name

/**
 * Base class for entity specific commands, providing / querying from user schema and entity as arguments
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.1
 */
abstract class AbstractSchemaCommand(name: String, help: String) : AbstractCottontailCommand(name = name, help = help) {
    /**
     * The [Name.SchemaName] affected by this [AbstractSchemaCommand].
     */
    protected val schemaName: Name.SchemaName by argument(name = "schema", help = "The schema name targeted by the command. Has the form of [\"warren\"].<schema>.").convert {
        Name.SchemaName(*it.split(Name.NAME_COMPONENT_DELIMITER).toTypedArray())
    }
}