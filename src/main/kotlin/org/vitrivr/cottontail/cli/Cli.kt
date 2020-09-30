package org.vitrivr.cottontail.cli

import com.github.ajalt.clikt.core.CliktCommand
import org.jline.reader.EndOfFileException
import org.jline.reader.LineReaderBuilder
import org.jline.reader.impl.completer.AggregateCompleter
import org.jline.reader.impl.completer.ArgumentCompleter
import org.jline.reader.impl.completer.NullCompleter
import org.jline.reader.impl.completer.StringsCompleter
import org.jline.terminal.Terminal
import org.jline.terminal.TerminalBuilder
import org.vitrivr.cottontail.server.grpc.CottontailGrpcServer
import java.io.IOException
import java.util.*
import java.util.regex.Pattern
import kotlin.time.ExperimentalTime

/**
 * Command line interface instance.  Setup and general parsing. Actual commands are implemented as
 * dedicated classes.
 *
 * This is basically a port of https://github.com/lucaro/DRES 's CLI
 *
 * @author Loris Sauter
 * @version 1.0
 */
@ExperimentalTime
object Cli {

    /** The default prompt -- just fancification */
    private const val PROMPT = "cottontaildb> "

    /** CottotanilGrpcServer instance; user for gracefully stopping the CLI. */
    lateinit var cottontailServer: CottontailGrpcServer


    private val completer: DelegateCompleter = DelegateCompleter(AggregateCompleter(StringsCompleter("help")))

    private lateinit var clikt: CliktCommand

    /**
     * Add the given list of completion candidates to completion
     */
    fun updateCompletion(strings: List<String>) {
        if (::clikt.isInitialized) {
            completer.delegate = AggregateCompleter(
                    StringsCompleter("help"),
                    StringsCompleter(clikt.registeredSubcommandNames()),
                    StringsCompleter(strings))
        } else {
            completer.delegate = AggregateCompleter(
                    StringsCompleter("help"),
                    StringsCompleter(strings)
            )
        }
    }

    /**
     * Adds dedicated argument completers to the completion
     */
    fun updateArgumentCompletion(schemata: List<String>, entities: List<String>) {
        val args = ArgumentCompleter(
                StringsCompleter(clikt.registeredSubcommandNames()),// should be safe to call
                StringsCompleter(schemata),
                StringsCompleter(entities),
                NullCompleter()
        )
        args.setStrictCommand(true)
        args.isStrict = false
        completer.delegate = AggregateCompleter(
                StringsCompleter("help"),
                args
        )
    }


    /**
     * Resets the autocompletion.
     * Resetting means, that in case the commands are already loaded, these are set plus "help"
     * Otherwise only "help" is added to the completion.
     */
    fun resetCompletion() {
        if (::clikt.isInitialized) {
            completer.delegate = AggregateCompleter(
                    StringsCompleter("help"),
                    StringsCompleter(clikt.registeredSubcommandNames()))
        } else {
            completer.delegate = AggregateCompleter(StringsCompleter("help"))
        }
    }

    /**
     * Blocking REPL of the CLI
     */
    fun loop(host: String = "localhost", port: Int = 1865) {
        clikt = CottontailCommand(host, port)

        val terminal: Terminal?
        try {
            terminal = TerminalBuilder.terminal()
        } catch (e: IOException) {
            error("Could not init terminal for reason:\n" +
                    "${e.message}\n" +
                    "Exiting...")
        }

        (clikt as CottontailCommand).initCompletion()

        val lineReader = LineReaderBuilder.builder().terminal(terminal).completer(completer).build()

        while (true) {
            /* Catch ^D end of file as exit method */
            val line = try {
                lineReader.readLine(PROMPT).trim()
            } catch (e: EndOfFileException) {
                "stop"
            }
            if (line.toLowerCase() == "help") {
                println(clikt.getFormattedHelp())
                continue
            }
            if (line.isBlank()) {
                continue
            }
            try {
                clikt.parse(splitLine(line))
                println()
            } catch (e: Exception) {
                when (e) {
                    is com.github.ajalt.clikt.core.NoSuchSubcommand -> println("command not found")
                    is com.github.ajalt.clikt.core.PrintHelpMessage -> println(e.command.getFormattedHelp())
                    is com.github.ajalt.clikt.core.MissingParameter -> println(e.localizedMessage)
                    is com.github.ajalt.clikt.core.NoSuchOption -> println(e.localizedMessage)
                    else -> e.printStackTrace()
                }
            }
        }

    }

    val lineSplitRegex: Pattern = Pattern.compile("[^\\s\"']+|\"([^\"]*)\"|'([^']*)'")

    //based on https://stackoverflow.com/questions/366202/regex-for-splitting-a-string-using-space-when-not-surrounded-by-single-or-double/366532
    private fun splitLine(line: String?): List<String> {
        if (line == null || line.isEmpty()) {
            return emptyList()
        }
        val matchList: MutableList<String> = ArrayList()
        val regexMatcher = lineSplitRegex.matcher(line)
        while (regexMatcher.find()) {
            if (regexMatcher.group(1) != null) {
                // Add double-quoted string without the quotes
                matchList.add(regexMatcher.group(1))
            } else if (regexMatcher.group(2) != null) {
                // Add single-quoted string without the quotes
                matchList.add(regexMatcher.group(2))
            } else {
                // Add unquoted word
                matchList.add(regexMatcher.group())
            }
        }
        return matchList
    }

    fun stopServer() {
        if (::cottontailServer.isInitialized) {
            cottontailServer.stop()
        }
    }
}