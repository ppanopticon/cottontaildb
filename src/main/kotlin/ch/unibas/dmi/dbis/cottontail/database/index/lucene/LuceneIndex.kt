package ch.unibas.dmi.dbis.cottontail.database.index.lucene

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnType
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.events.DataChangeEvent
import ch.unibas.dmi.dbis.cottontail.database.events.DataChangeEventType
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.index.IndexTransaction
import ch.unibas.dmi.dbis.cottontail.database.index.IndexType
import ch.unibas.dmi.dbis.cottontail.database.index.hash.UniqueHashIndex
import ch.unibas.dmi.dbis.cottontail.database.queries.*
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.exceptions.ValidationException
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.recordset.StandaloneRecord
import ch.unibas.dmi.dbis.cottontail.model.values.FloatValue
import ch.unibas.dmi.dbis.cottontail.model.values.StringValue
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.*
import org.apache.lucene.index.*
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.TermQuery
import org.apache.lucene.search.TermRangeQuery
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.store.NativeFSLockFactory
import org.slf4j.LoggerFactory
import java.nio.file.Path

/**
 * Represents a Apache Lucene based index in the Cottontail DB data model. The [LuceneIndex] allows for string comparisons using the LIKE operator.
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 1.0
 */
class LuceneIndex(override val name: Name, override val parent: Entity, override val columns: Array<ColumnDef<*>>) : Index() {

    companion object {
        /** Cost of a single lookup operation (i.e. comparison of a term in the index). */
        const val ATOMIC_COST = 1e-6f

        /** [ColumnDef] of the _tid column. */
        const val TID_COLUMN = "_tid"

        /**
         * Maps a [Record] to a [Document] that can be processed by Lucene.
         *
         * @param record The [Record]
         * @return The resulting [Document]
         */
        private fun documentFromRecord(record: Record): Document = Document().apply {
            add(NumericDocValuesField(TID_COLUMN, record.tupleId))
            add(StoredField(TID_COLUMN, record.tupleId))
            record.columns.forEach {
                val value = record[it]
                if (value is StringValue) {
                    add(TextField("${it.name}_txt", value.value, Field.Store.NO))
                    add(StringField("${it.name}_str", value.value, Field.Store.NO))
                }
            }
        }

        private val LOGGER = LoggerFactory.getLogger(LuceneIndex::class.java)
    }

    /** Constant FQN of the [Schema] object. */
    override val fqn: Name = this.parent.fqn.append(this.name)

    /** The [LuceneIndex] implementation produces an additional score column. */
    override val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef(parent.fqn.append("score"), ColumnType.forName("FLOAT")))

    /** The path to the directory that contains the data for this [LuceneIndex]. */
    override val path: Path = this.parent.path.resolve("idx_lucene_$name")

    /** The type of this [Index]. */
    override val type: IndexType = IndexType.LUCENE

    /** Flag indicating whether or not this [LuceneIndex] is open and usable. */
    @Volatile
    override var closed: Boolean = false
        private set

    /** The [Directory] containing the data for this [LuceneIndex]. */
    private val directory: Directory = FSDirectory.open(this.path, NativeFSLockFactory.getDefault())

    /** Initial commit in case writer was created. */
    init {
        val writer = IndexWriter(this.directory, IndexWriterConfig(StandardAnalyzer()).setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND).setCommitOnClose(true))
        writer.close()
    }

    /** The [IndexReader] instance used for accessing the [LuceneIndex]. */
    private var indexReader = DirectoryReader.open(this.directory)

    /**
     * Returns true, if the [LuceneIndex] supports incremental updates, and false otherwise.
     *
     * @return True if incremental [Index] updates are supported.
     */
    override fun supportsIncrementalUpdate(): Boolean = false /** TODO: Add support. */

    /**
     * (Re-)builds the [LuceneIndex].
     *
     * @param tx Reference to the [Entity.Tx] the call to this method belongs to.
     */
    override fun rebuild(tx: Entity.Tx) {
        LOGGER.trace("Rebuilding lucene index {}", name)
        val writer = IndexWriter(this.directory, IndexWriterConfig(StandardAnalyzer()).setOpenMode(IndexWriterConfig.OpenMode.APPEND).setMaxBufferedDocs(100_000).setCommitOnClose(true))
        writer.deleteAll()
        var count = 0
        tx.forEach {
            writer.addDocument(documentFromRecord(it))
            count++
        }
        writer.close()

        /* Open new IndexReader and close new one. */
        val oldReader = this.indexReader
        this.indexReader = DirectoryReader.open(this.directory)
        oldReader.close()
    }

    /**
     * Updates the [LuceneIndex] with the provided [Record]. This method determines, whether the [Record] should be added or updated
     *
     * @param update [DataChangeEvent]s based on which to update the [LuceneIndex].
     * @param tx Reference to the [Entity.Tx] the call to this method belongs to.
     * @throws [ValidationException.IndexUpdateException] If rebuild of [Index] fails for some reason.
     */
    override fun update(update: Collection<DataChangeEvent>, tx: Entity.Tx) {
       val writer = IndexWriter(this.directory, IndexWriterConfig(StandardAnalyzer()).setOpenMode(IndexWriterConfig.OpenMode.APPEND).setMaxBufferedDocs(100_000).setCommitOnClose(true))

        /* Define action for inserting an entry based on a DataChangeEvent. */
        fun atomicInsert(event: DataChangeEvent) {
            writer.addDocument(documentFromRecord(event.new!!))
        }


        /* Define action for deleting an entry based on a DataChangeEvent. */
        fun atomicDelete(event: DataChangeEvent){
            writer.deleteDocuments(NumericDocValuesField.newSlowExactQuery(TID_COLUMN, event.old!!.tupleId))
        }

        /* Process the DataChangeEvents. */
        for (event in update) {
            when (event.type) {
                DataChangeEventType.INSERT -> atomicInsert(event)
                DataChangeEventType.UPDATE -> {
                    if (event.new?.get(this.columns[0]) != event.old?.get(this.columns[0])) {
                        atomicDelete(event)
                        atomicInsert(event)
                    }
                }
                DataChangeEventType.DELETE -> atomicDelete(event)
                else -> {}
            }
        }

        val oldReader = this.indexReader
        this.indexReader = DirectoryReader.open(this.directory)
        oldReader.close()
    }


    /**
     * Closes this [LuceneIndex] and the associated data structures.
     */
    override fun close() = this.globalLock.write {
        if (!this.closed) {
            this.indexReader.close()
            this.directory.close()
            this.closed = true
        }
    }

    /**
     * Performs a lookup through this [LuceneIndex] and returns [Recordset] containing only the [Record]'s tuple ID. This is an internal method
     * External invocation is only possible through a [Index.Tx] object.
     *
     * @param predicate The [Predicate] to perform the lookup. Must be a LIKE query.
     * @param tx Reference to the [Entity.Tx] the call to this method belongs to.
     * @return The resulting [Recordset].
     *
     * @throws DatabaseException.PredicateNotSupportedBxIndexException If predicate is not supported by [Index].
     */
    override fun filter(predicate: Predicate, tx: Entity.Tx): Recordset = if (predicate is BooleanPredicate) {
        val indexSearcher = IndexSearcher(this.indexReader)
        if (!predicate.columns.all { this.columns.contains(it) })
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) is lacking certain fields the provided predicate requires.")

        if (!predicate.atomics.all { it.operator == ComparisonOperator.LIKE || it.operator == ComparisonOperator.EQUAL })
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) can only process LIKE comparisons.")

        /* Generate query. */
        val query = when (predicate) {
            is AtomicBooleanPredicate<*> -> predicate.toLuceneQuery()
            is CompoundBooleanPredicate -> predicate.toLuceneQuery()
        }

        /* Construct empty Recordset. */
        val resultset = Recordset(columns = arrayOf(*this.produces))

        /* Execute query and add results. */
        val results = indexSearcher.search(query, Integer.MAX_VALUE)
        results.scoreDocs.forEach { sdoc ->
            val doc = indexSearcher.doc(sdoc.doc)
            resultset.addRowUnsafe(doc[TID_COLUMN].toLong(), values = arrayOf(FloatValue(sdoc.score)))
        }
        resultset
    } else {
        throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene index) does not support predicates of type '${predicate::class.simpleName}'.")
    }

    /**
     * Applies the given action to all the [LuceneIndex] entries that match the given [Predicate]. This is an internal method!
     * External invocation is only possible through a [Index.Tx] object.
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @param tx Reference to the [Entity.Tx] the call to this method belongs to.
     * @param action The action that should be applied.
     *
     * @throws DatabaseException.PredicateNotSupportedBxIndexException If predicate is not supported by [Index].
     */
    override fun forEach(predicate: Predicate, tx: Entity.Tx, action: (Record) -> Unit) = if (predicate is BooleanPredicate) {
        val indexSearcher = IndexSearcher(this.indexReader)

        if (!predicate.columns.all { this.columns.contains(it) })
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) is lacking certain fields the provided predicate requires.")

        if (!predicate.atomics.all { it.operator == ComparisonOperator.LIKE || it.operator == ComparisonOperator.EQUAL })
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) can only process LIKE comparisons.")

        /* Generate query. */
        val query = when (predicate) {
            is AtomicBooleanPredicate<*> -> predicate.toLuceneQuery()
            is CompoundBooleanPredicate -> predicate.toLuceneQuery()
        }

        /* Execute query and add results. */
        val results = indexSearcher.search(query, Integer.MAX_VALUE)
        results.scoreDocs.forEach { sdoc ->
            val doc = indexSearcher.doc(sdoc.doc)
            action(StandaloneRecord(tupleId = doc[TID_COLUMN].toLong(), columns = arrayOf(*this.produces)).assign(arrayOf(FloatValue(sdoc.score))))
        }
    } else {
        throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene index) does not support predicates of type '${predicate::class.simpleName}'.")
    }

    /**
     * Applies the given mapping function to all the [LuceneIndex] entries that match the given [Predicate]. This is an internal method!
     * External invocation is only possible through a [Index.Tx] object.
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @param tx Reference to the [Entity.Tx] the call to this method belongs to.
     * @param action The action that should be applied.
     *
     * @throws DatabaseException.PredicateNotSupportedBxIndexException If predicate is not supported by [Index].
     */
    override fun <R> map(predicate: Predicate, tx: Entity.Tx, action: (Record) -> R): Collection<R> = if (predicate is BooleanPredicate) {
        val indexSearcher = IndexSearcher(this.indexReader)

        if (!predicate.columns.all { this.columns.contains(it) })
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) is lacking certain fields the provided predicate requires.")

        if (!predicate.atomics.all { it.operator == ComparisonOperator.LIKE || it.operator == ComparisonOperator.EQUAL })
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) can only process LIKE comparisons.")

        /* Generate query. */
        val query = when (predicate) {
            is AtomicBooleanPredicate<*> -> predicate.toLuceneQuery()
            is CompoundBooleanPredicate -> predicate.toLuceneQuery()
        }

        /* Execute query and add results. */
        indexSearcher.search(query, Integer.MAX_VALUE).scoreDocs.map { sdoc ->
            val doc = indexSearcher.doc(sdoc.doc)
            action(StandaloneRecord(tupleId = doc[TID_COLUMN].toLong(), columns = arrayOf(*produces)).assign(arrayOf(FloatValue(sdoc.score))))
        }
    } else {
        throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene index) does not support predicates of type '${predicate::class.simpleName}'.")
    }

    /**
     * Checks if this [LuceneIndex] can process the given [Predicate].
     *
     * @param predicate [Predicate] to test.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean = if (predicate is BooleanPredicate) {
        predicate.columns.all { this.columns.contains(it) } && predicate.atomics.all { it.operator == ComparisonOperator.LIKE || it.operator == ComparisonOperator.EQUAL }
    } else {
        false
    }

    /**
     * Calculates the cost estimate of this [UniqueHashIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Float = when {
        canProcess(predicate) -> {
            val searcher = IndexSearcher(this.indexReader)
            predicate.columns.map { searcher.collectionStatistics(it.name.name).sumTotalTermFreq() * ATOMIC_COST }.sum()
        }
        else -> Float.MAX_VALUE
    }
}
