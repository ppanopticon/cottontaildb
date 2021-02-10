package org.vitrivr.cottontail.database.index.lucene

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.phonetic.DoubleMetaphoneFilter
import org.apache.lucene.analysis.standard.StandardTokenizer


class SoundexAnalyzer : Analyzer() {
    override fun createComponents(fieldName: String?): TokenStreamComponents {
        val tokenizer = StandardTokenizer()
        val stream = DoubleMetaphoneFilter(tokenizer, 6, false)
        return TokenStreamComponents(tokenizer, stream)
    }
}