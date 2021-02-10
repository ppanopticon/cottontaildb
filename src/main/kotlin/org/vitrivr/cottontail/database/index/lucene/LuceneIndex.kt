package org.vitrivr.cottontail.database.index.lucene

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.*
import org.apache.lucene.index.*
import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil
import org.apache.lucene.search.*
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.store.NativeFSLockFactory
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.index.hash.UniqueHashIndex
import org.vitrivr.cottontail.database.index.lsh.superbit.SuperBitLSHIndex
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.FloatValue
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.pattern.LikePatternValue
import org.vitrivr.cottontail.model.values.pattern.LucenePatternValue
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Path

/**
 * An Apache Lucene based [Index]. The [LuceneIndex] allows for fast search on text using the EQUAL
 * or LIKE operator.
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 1.4.0
 */
class LuceneIndex(
    override val name: Name.IndexName,
    override val parent: Entity,
    override val columns: Array<ColumnDef<*>>,
    override val path: Path,
    config: LuceneIndexConfig? = null
) : Index() {

    companion object {
        /** [ColumnDef] of the _tid column. */
        const val TID_COLUMN = "_tid"

        private const val LUCENE_INDEX_CONFIG = "lucene_config"

        /** The [ComparisonOperator]s supported by this [LuceneIndex]. */
        private val SUPPORTS =
            arrayOf(ComparisonOperator.LIKE, ComparisonOperator.EQUAL, ComparisonOperator.MATCH)
    }

    /** The [LuceneIndex] implementation produces an additional score column. */
    override val produces: Array<ColumnDef<*>> =
        arrayOf(ColumnDef(this.parent.name.column("score"), Type.Float))

    /** True since [SuperBitLSHIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** False, since [LuceneIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    /** Always false, due to incremental updating being supported. */
    override val dirty: Boolean = false

    /** The type of this [Index]. */
    override val type: IndexType = IndexType.LUCENE

    /** The [LuceneIndexConfig] used by this [LuceneIndex] instance. */
    private val config: LuceneIndexConfig

    /** Flag indicating whether or not this [LuceneIndex] is open and usable. */
    @Volatile
    override var closed: Boolean = false
        private set

    /** The [Directory] containing the data for this [LuceneIndex]. */
    private val directory: Directory = FSDirectory.open(this.path, NativeFSLockFactory.getDefault())

    init {
        /** Tries to obtain config from disk. */
        val db = this.parent.parent.parent.config.mapdb.db(this.path.resolve("config.db"))
        val configOnDisk =
            db.atomicVar(LUCENE_INDEX_CONFIG, LuceneIndexConfig.Serializer).createOrOpen()
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
        val writer = IndexWriter(
            this.directory,
            IndexWriterConfig(this.config.getAnalyzer()).setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
                .setCommitOnClose(true)
        )
        writer.close()
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
                    predicate.atomics.all { it.operator in SUPPORTS }

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
            predicate.columns.forEach {
                cost += Cost(
                    Cost.COST_DISK_ACCESS_READ,
                    Cost.COST_DISK_ACCESS_READ,
                    it.type.physicalSize.toFloat()
                ) * searcher.collectionStatistics(it.name.simple).sumTotalTermFreq()
            }
            cost
        }
        else -> Cost.INVALID
    }

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [Index].
     *
     * @param context If the [TransactionContext] to create the [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * Closes this [LuceneIndex] and the associated data structures.
     */
    override fun close() = this.closeLock.write {
        if (!this.closed) {
            this.indexReader.close()
            this.directory.close()
            this.closed = true
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
        is BooleanPredicate.Atomic -> this.toLuceneQuery()
        is BooleanPredicate.Compound -> this.toLuceneQuery()
    }

    /**
     * Converts an [BooleanPredicate.Atomic] to a [Query] supported by Apache Lucene.
     * Conversion differs slightly depending on the [ComparisonOperator].
     *
     * @return [Query]
     */
    private fun BooleanPredicate.Atomic.toLuceneQuery(): Query = when (this.operator) {
        ComparisonOperator.EQUAL -> {
            val column = this.columns.first()
            val string = this.values.first()
            if (string is StringValue) {
                TermQuery(Term("${column.name}_str", string.value))
            } else {
                throw throw QueryException("Conversion to Lucene query failed: EQUAL queries strictly require a StringValue as second operand!")
            }
        }
        ComparisonOperator.LIKE -> {
            val column = this.columns.first()
            when (val pattern = this.values.first()) {
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
        ComparisonOperator.MATCH -> {
            val column = this.columns.first()
            val pattern = this.values.first()
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
    private inner class Tx(context: TransactionContext) : Index.Tx(context) {

        /** The [IndexWriter] instance used to access this [LuceneIndex]. */
        private val writer = IndexWriter(
            this@LuceneIndex.directory,
            IndexWriterConfig(this@LuceneIndex.config.getAnalyzer()).setOpenMode(IndexWriterConfig.OpenMode.APPEND)
                .setMaxBufferedDocs(100_000).setCommitOnClose(false)
        )

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
            /* Obtain Tx for parent [Entity. */
            val entityTx = this.context.getTx(this.dbo.parent) as EntityTx

            /* Recreate entries. */
            this.writer.deleteAll()
            entityTx.scan(this@LuceneIndex.columns).use { s ->
                s.forEach { record ->
                    this.writer.addDocument(documentFromRecord(record))
                }
            }
        }

        /**
         * Updates the [LuceneIndex] with the provided [DataChangeEvent].
         *
         * @param event [DataChangeEvent] to process.
         */
        override fun update(event: DataChangeEvent) = this.withWriteLock {
            when (event) {
                is DataChangeEvent.InsertDataChangeEvent -> {
                    val new = event.inserts[this.columns[0]]
                    if (new is StringValue) {
                        this.writer.addDocument(
                            this@LuceneIndex.documentFromValue(
                                new,
                                event.tupleId
                            )
                        )
                    }
                }
                is DataChangeEvent.UpdateDataChangeEvent -> {
                    this.writer.deleteDocuments(Term(TID_COLUMN, event.tupleId.toString()))
                    val new = event.updates[this.columns[0]]?.second
                    if (new is StringValue) {
                        this.writer.addDocument(
                            this@LuceneIndex.documentFromValue(
                                new,
                                event.tupleId
                            )
                        )
                    }
                }
                is DataChangeEvent.DeleteDataChangeEvent -> {
                    this.writer.deleteDocuments(Term(TID_COLUMN, event.tupleId.toString()))
                }
            }
            Unit
        }


        /**
         * Performs a lookup through this [LuceneIndex.Tx] and returns a [CloseableIterator] of
         * all [TupleId]s that match the [Predicate]. Only supports [BooleanPredicate]s.
         *
         * The [CloseableIterator] is not thread safe!
         *
         * <strong>Important:</strong> It remains to the caller to close the [CloseableIterator]
         *
         * @param predicate The [Predicate] for the lookup*
         * @return The resulting [CloseableIterator]
         */
        override fun filter(predicate: Predicate) = object : CloseableIterator<Record> {
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

            /** Number of [TupleId]s returned by this [CloseableIterator]. */
            @Volatile
            private var returned = 0

            /** Flag indicating whether this [CloseableIterator] has been closed. */
            @Volatile
            override var isOpen = true
                private set

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
                check(this.isOpen) { "Illegal invocation of next(): This CloseableIterator has been closed." }
                return this.returned < this.results.totalHits
            }

            /**
             * Returns the next element in the iteration.
             */
            override fun next(): Record {
                check(this.isOpen) { "Illegal invocation of next(): This CloseableIterator has been closed." }
                val scores = this.results.scoreDocs[this.returned++]
                val doc = this.searcher.doc(scores.doc)
                return StandaloneRecord(doc[TID_COLUMN].toLong(), this@LuceneIndex.produces, arrayOf(FloatValue(scores.score)))
            }

            /**
             * Closes this [CloseableIterator] and releases all locks and resources associated with it.
             */
            override fun close() {
                if (this.isOpen) {
                    this.isOpen = false
                }
            }
        }

        /**
         * The [LuceneIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] to perform the lookup.
         * @param range The [LongRange] to consider.
         * @return The resulting [CloseableIterator].
         */
        override fun filterRange(
            predicate: Predicate,
            range: LongRange
        ): CloseableIterator<Record> {
            throw UnsupportedOperationException("The LuceneIndex does not support ranged filtering!")
        }

        /** Performs the actual COMMIT operation by committing the [IndexWriter] and updating the [IndexReader]. */
        override fun performCommit() {
            /* Commits changes made through the LuceneWriter. */
            this.writer.commit()

            /* Opens new IndexReader and close new one. */
            val oldReader = this@LuceneIndex.indexReader
            this@LuceneIndex.indexReader = DirectoryReader.open(this@LuceneIndex.directory)
            oldReader.close()
        }

        /** Performs the actual ROLLBACK operation by rolling back the [IndexWriter]. */
        override fun performRollback() {
            this.writer.rollback()
        }

        /** Makes the necessary cleanup by closing the [IndexWriter]. */
        override fun cleanup() {
            this.writer.close()
            super.cleanup()
        }
    }
}
