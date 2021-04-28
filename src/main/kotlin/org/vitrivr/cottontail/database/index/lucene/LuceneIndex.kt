package org.vitrivr.cottontail.database.index.lucene

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.*
import org.apache.lucene.index.*
import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil
import org.apache.lucene.search.*
import org.apache.lucene.search.similarities.SimilarityBase.log2
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.store.NativeFSLockFactory
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.general.TxAction
import org.vitrivr.cottontail.database.general.TxSnapshot
import org.vitrivr.cottontail.database.index.AbstractIndex
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.index.hash.UniqueHashIndex
import org.vitrivr.cottontail.database.index.lsh.superbit.SuperBitLSHIndex
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.database.queries.predicates.bool.ConnectionOperator
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.FloatValue
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.pattern.LikePatternValue
import org.vitrivr.cottontail.model.values.pattern.LucenePatternValue
import java.nio.file.Path

/**
 * An Apache Lucene based [AbstractIndex]. The [LuceneIndex] allows for fast search on text using the EQUAL
 * or LIKE operator.
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 2.0.1
 */
class LuceneIndex(path: Path, parent: DefaultEntity, config: LuceneIndexConfig? = null) :
    AbstractIndex(path, parent) {

    companion object {
        /** [ColumnDef] of the _tid column. */
        const val TID_COLUMN = "_tid"
    }

    /** The [LuceneIndex] implementation produces an additional score column. */
    override val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef(this.parent.name.column("score"), Type.Float))

    /** True since [SuperBitLSHIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** False, since [LuceneIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    /** The type of this [AbstractIndex]. */
    override val type: IndexType = IndexType.LUCENE

    /** The [LuceneIndexConfig] used by this [LuceneIndex] instance. */
    override val config: LuceneIndexConfig

    /** The [Directory] containing the data for this [LuceneIndex]. */
    private val directory: Directory = FSDirectory.open(
        this.path.parent.resolve("${this.name.simple}.lucene"),
        NativeFSLockFactory.getDefault()
    )

    init {
        /** Tries to obtain config from disk. */
        val configOnDisk =
            this.store.atomicVar(INDEX_CONFIG_FIELD, LuceneIndexConfig.Serializer).createOrOpen()
        if (configOnDisk.get() == null) {
            if (config != null) {
                this.config = config
            } else {
                this.config = LuceneIndexConfig(LuceneAnalyzerType.STANDARD)
            }
            configOnDisk.set(config)
        } else {
            this.config = configOnDisk.get()
        }

        /** Initial commit of write in case writer was created freshly. */
        val writer = IndexWriter(this.directory, IndexWriterConfig(this.config.getAnalyzer()).setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND).setCommitOnClose(true))
        writer.close()

        /* Initial commit of store. */
        this.store.commit()
    }

    /** The [IndexReader] instance used for accessing the [LuceneIndex]. */
    private var indexReader = DirectoryReader.open(this.directory)

    /**
     * Checks if this [LuceneIndex] can process the given [Predicate].
     *
     * @param predicate [Predicate] to test.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean =
        predicate is BooleanPredicate &&
                predicate.columns.all { it in this.columns } &&
                predicate.atomics.all { it.operator is ComparisonOperator.Binary.Like || it.operator is ComparisonOperator.Binary.Equal || it.operator is ComparisonOperator.Binary.Match }

    /**
     * Calculates the cost estimate of this [UniqueHashIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost = when {
        canProcess(predicate) -> {
            val searcher = IndexSearcher(this.indexReader)
            var cost = Cost.ZERO
            repeat(predicate.columns.size) {
                cost += Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS) * log2(searcher.indexReader.numDocs().toDouble()) /* TODO: This is an assumption. */
            }
            cost
        }
        else -> Cost.INVALID
    }

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [AbstractIndex].
     *
     * @param context If the [TransactionContext] to create the [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * Closes this [LuceneIndex] and the associated data structures.
     */
    override fun close() {
        try {
            super.close()
        } finally {
            this.indexReader.close()
            this.directory.close()
        }
    }

    /**
     * Converts a [Record] to a [Document] that can be processed by Lucene.
     *
     * @param record The [Record]
     * @return The resulting [Document]
     */
    private fun documentFromRecord(record: Record): Document {
        val value = record[this.columns[0]]
        if (value is StringValue) {
            return documentFromValue(value, record.tupleId)
        } else {
            throw IllegalArgumentException("Given record does not contain a StringValue column named ${this.columns[0].name}.")
        }
    }

    /**
     * Converts a [StringValue] and a [TupleId] to [Document] that can be processed by Lucene.
     *
     * @param value: [StringValue] to process
     * @param tupleId The [TupleId] to process
     * @return The resulting [Document]
     */
    private fun documentFromValue(value: StringValue, tupleId: TupleId): Document {
        val doc = Document()
        doc.add(NumericDocValuesField(TID_COLUMN, tupleId))
        doc.add(StoredField(TID_COLUMN, tupleId))
        doc.add(TextField("${this.columns[0].name}_txt", value.value, Field.Store.NO))
        doc.add(StringField("${this.columns[0].name}_str", value.value, Field.Store.NO))
        return doc
    }

    /**
     * Converts a [BooleanPredicate] to a [Query] supported by Apache Lucene.
     *
     * @return [Query]
     */
    private fun BooleanPredicate.toLuceneQuery(): Query = when (this) {
        is BooleanPredicate.Atomic.Literal -> this.toLuceneQuery()
        is BooleanPredicate.Compound -> this.toLuceneQuery()
        is BooleanPredicate.Atomic.Reference -> throw IllegalStateException("An Atomic.Reference boolean predicate cannot be converted to a lucene query.")
    }

    /**
     * Converts an [BooleanPredicate.Atomic] to a [Query] supported by Apache Lucene.
     * Conversion differs slightly depending on the [ComparisonOperator].
     *
     * @return [Query]
     */
    private fun BooleanPredicate.Atomic.Literal.toLuceneQuery(): Query = when (this.operator) {
        is ComparisonOperator.Binary.Equal -> {
            val column = this.columns.first()
            val string = this.operator.right.value
            if (string is StringValue) {
                TermQuery(Term("${column.name}_str", string.value))
            } else {
                throw throw QueryException("Conversion to Lucene query failed: EQUAL queries strictly require a StringValue as second operand!")
            }
        }
        is ComparisonOperator.Binary.Like -> {
            val column = this.columns.first()
            when (val pattern = this.operator.right.value) {
                is LucenePatternValue -> QueryParserUtil.parse(
                    arrayOf(pattern.value),
                    arrayOf("${column.name}_txt"),
                    StandardAnalyzer()
                )
                is LikePatternValue -> QueryParserUtil.parse(
                    arrayOf(pattern.toLucene().value),
                    arrayOf("${column.name}_txt"),
                    StandardAnalyzer()
                )
                else -> throw throw QueryException("Conversion to Lucene query failed: LIKE queries require a LucenePatternValue OR LikePatternValue as second operand!")
            }
        }
        is ComparisonOperator.Binary.Match -> {
            val column = this.columns.first()
            val pattern = this.operator.right.value
            if (pattern is LucenePatternValue) {
                QueryParserUtil.parse(arrayOf(pattern.value), arrayOf("${column.name}_txt"), StandardAnalyzer())
            } else {
                throw throw QueryException("Conversion to Lucene query failed: MATCH queries strictly require a LucenePatternValue as second operand!")
            }
        }
        else -> throw QueryException("Lucene Query Conversion failed: Only EQUAL, MATCH and LIKE queries can be mapped to a Apache Lucene!")
    }

    /**
     * Converts a [BooleanPredicate.Compound] to a [Query] supported by Apache Lucene.
     *
     * @return [Query]
     */
    private fun BooleanPredicate.Compound.toLuceneQuery(): Query {
        val clause = when (this.connector) {
            ConnectionOperator.AND -> BooleanClause.Occur.MUST
            ConnectionOperator.OR -> BooleanClause.Occur.SHOULD
        }
        val builder = BooleanQuery.Builder()
        builder.add(this.p1.toLuceneQuery(), clause)
        builder.add(this.p2.toLuceneQuery(), clause)
        return builder.build()
    }

    /**
     * An [IndexTx] that affects this [LuceneIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {

        /** The [IndexWriter] instance used to access this [LuceneIndex]. */
        private var writer: IndexWriter? = null

        /** The default [TxSnapshot] of this [IndexTx].  */
        override val snapshot = object : TxSnapshot {
            override val actions: List<TxAction> = emptyList()

            /** Commits DB and Lucene writer and updates lucene reader. */
            override fun commit() {
                this@LuceneIndex.store.commit()
                if (this@Tx.writer?.hasUncommittedChanges() == true) {
                    this@Tx.writer?.commit()
                    val oldReader = this@LuceneIndex.indexReader
                    this@LuceneIndex.indexReader = DirectoryReader.open(this@LuceneIndex.directory)
                    oldReader.close()
                }
            }

            /** Rolls back DB and Lucene writer . */
            override fun rollback() {
                this@LuceneIndex.store.rollback()
                if (this@Tx.writer?.hasUncommittedChanges() == true) {
                    this@Tx.writer?.rollback()
                }
            }

            override fun record(action: TxAction): Boolean = false
        }

        /**
         * Returns the number of [Document] in this [LuceneIndex], which should roughly correspond
         * to the number of [TupleId]s it contains.
         *
         * @return Number of [Document]s in this [LuceneIndex]
         */
        override fun count(): Long = this.withReadLock {
            return this@LuceneIndex.indexReader.numDocs().toLong()
        }

        /**
         * (Re-)builds the [LuceneIndex].
         */
        override fun rebuild() = this.withWriteLock {
            this.ensureWriterAvailable()

            /* Obtain Tx for parent [Entity. */
            val entityTx = this.context.getTx(this.dbo.parent) as EntityTx

            /* Recreate entries. */
            this.writer?.deleteAll()
            entityTx.scan(this@LuceneIndex.columns).forEach { record ->
                this.writer?.addDocument(documentFromRecord(record))
            }
        }

        /**
         * Updates the [LuceneIndex] with the provided [DataChangeEvent].
         *
         * @param event [DataChangeEvent] to process.
         */
        override fun update(event: DataChangeEvent) = this.withWriteLock {
            this.ensureWriterAvailable()
            when (event) {
                is DataChangeEvent.InsertDataChangeEvent -> {
                    val new = event.inserts[this.dbo.columns[0]]
                    if (new is StringValue) {
                        this.writer?.addDocument(this@LuceneIndex.documentFromValue(new, event.tupleId))
                    }
                }
                is DataChangeEvent.UpdateDataChangeEvent -> {
                    this.writer?.deleteDocuments(Term(TID_COLUMN, event.tupleId.toString()))
                    val new = event.updates[this.dbo.columns[0]]?.second
                    if (new is StringValue) {
                        this.writer?.addDocument(this@LuceneIndex.documentFromValue(new, event.tupleId))
                    }
                }
                is DataChangeEvent.DeleteDataChangeEvent -> {
                    this.writer?.deleteDocuments(Term(TID_COLUMN, event.tupleId.toString()))
                }
            }
            Unit
        }

        /**
         * Clears the [LuceneIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.withWriteLock {
            this.ensureWriterAvailable()
            this.writer?.deleteAll()
            this@LuceneIndex.dirtyField.compareAndSet(false, true)
            Unit
        }

        /**
         * Performs a lookup through this [LuceneIndex.Tx] and returns a [Iterator] of
         * all [TupleId]s that match the [Predicate]. Only supports [BooleanPredicate]s.
         *
         * The [Iterator] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup*
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate) = object : Iterator<Record> {
            /** Cast [BooleanPredicate] (if such a cast is possible). */
            private val predicate = if (predicate !is BooleanPredicate) {
                throw QueryException.UnsupportedPredicateException("Index '${this@LuceneIndex.name}' (lucene index) does not support predicates of type '${predicate::class.simpleName}'.")
            } else {
                predicate
            }

            /* Performs some sanity checks. */
            init {
                if (!this@LuceneIndex.canProcess(predicate)) {
                    throw QueryException.UnsupportedPredicateException("Index '${this@LuceneIndex.name}' (lucene-index) cannot process the provided predicate.")
                }
                this@Tx.withReadLock { }
            }

            /** Number of [TupleId]s returned by this [Iterator]. */
            @Volatile
            private var returned = 0

            /** Lucene [Query] representation of [BooleanPredicate] . */
            private val query: Query = this.predicate.toLuceneQuery()

            /** [IndexSearcher] instance used for lookup. */
            private val searcher = IndexSearcher(this@LuceneIndex.indexReader)

            /* Execute query and add results. */
            private val results = this.searcher.search(this.query, Integer.MAX_VALUE)

            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean {
                return this.returned < this.results.totalHits.value
            }

            /**
             * Returns the next element in the iteration.
             */
            override fun next(): Record {
                val scores = this.results.scoreDocs[this.returned++]
                val doc = this.searcher.doc(scores.doc)
                return StandaloneRecord(doc[TID_COLUMN].toLong(), this@LuceneIndex.produces, arrayOf(FloatValue(scores.score)))
            }
        }

        /**
         * The [LuceneIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partitionIndex The [partitionIndex] for this [filterRange] call.
         * @param partitions The total number of partitions for this [filterRange] call.
         * @return The resulting [Iterator].
         */
        override fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int): Iterator<Record> {
            throw UnsupportedOperationException("The LuceneIndex does not support ranged filtering!")
        }

        /** Makes the necessary cleanup by closing the [IndexWriter]. */
        override fun cleanup() {
            this.writer?.close()
            super.cleanup()
        }

        /** Makes sure that an [IndexWriter] instance is available. */
        private fun ensureWriterAvailable() {
            if (this.writer == null) {
                val config = IndexWriterConfig(this@LuceneIndex.config.getAnalyzer()).setOpenMode(IndexWriterConfig.OpenMode.APPEND).setMaxBufferedDocs(10000).setCommitOnClose(false)
                this.writer = IndexWriter(this@LuceneIndex.directory, config)
            }
        }
    }
}
