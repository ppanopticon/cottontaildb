package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.model.basics.Name
import kotlin.time.ExperimentalTime

/**
 * Base class for entity specific commands, providing / querying from user schema and entity as arguments
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
abstract class AbstractEntityCommand(name: String, help: String) : AbstractCottontailCommand(name = name, help = help) {
    /**
     * The [Name.EntityName] affected by this [AbstractEntityCommand].
     */
    protected val entityName: Name.EntityName by argument(name = "entity", help = "The fully qualified entity name targeted by the command. Has the form of [\"warren\"].<schema>.<entity>").convert {
        Name.EntityName(*it.split(Name.NAME_COMPONENT_DELIMITER).toTypedArray())
    }
}