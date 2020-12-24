package org.vitrivr.cottontail.cli

import com.github.ajalt.clikt.core.CliktCommand
import io.grpc.StatusRuntimeException

/**
 * Base class for none entity specific commands (for potential future generalisation)

 * @author Loris Sauter
 * @version 1.0.0
 */
abstract class AbstractCottontailCommand(name: String, help: String) : CliktCommand(name = name, help = help) {
    /**
     * The actual command execution. Override this for your command
     */
    abstract fun exec()

    /**
     * Runs the command by calling [AbstractCottontailCommand.exec] safely. Exceptions are caught and printed.
     */
    override fun run() = try {
        exec()
    } catch (e: StatusRuntimeException) {
        println("Command execution failed: ${e.message}")
    }
}