package org.vitrivr.cottontail.cli

import org.jline.reader.Candidate
import org.jline.reader.Completer
import org.jline.reader.LineReader
import org.jline.reader.ParsedLine

class DelegateCompleter(var delegate: Completer) : Completer {
    override fun complete(reader: LineReader?, line: ParsedLine?, candidates: MutableList<Candidate>?) {
        delegate.complete(reader, line, candidates)
    }
}